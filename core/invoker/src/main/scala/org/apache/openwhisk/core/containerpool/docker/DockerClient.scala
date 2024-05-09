/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool.docker

import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.Semaphore

import akka.actor.ActorSystem

import scala.collection.concurrent.TrieMap
import scala.concurrent.blocking
import scala.concurrent.ExecutionContext
import scala.concurrent.{Await, Future}
import scala.util.Failure
import scala.util.Success
import scala.util.Try
//import akka.event.Logging.{ErrorLevel, InfoLevel}
import pureconfig._
import pureconfig.generic.auto._
//import org.apache.openwhisk.common.{Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.ContainerId
import org.apache.openwhisk.core.containerpool.ContainerAddress

import scala.concurrent.duration.Duration

object DockerContainerId {

  val containerIdRegex = """^([0-9a-f]{64})$""".r

  def parse(id: String): Try[ContainerId] = {
    id match {
      case containerIdRegex(_) => Success(ContainerId(id))
      case _                   => Failure(new IllegalArgumentException(s"Does not comply with Docker container ID format: ${id}"))
    }
  }
}

/**
 * Configuration for docker client command timeouts.
 */
case class DockerClientTimeoutConfig(run: Duration,
                                     rm: Duration,
                                     pull: Duration,
                                     ps: Duration,
                                     pause: Duration,
                                     unpause: Duration,
                                     version: Duration,
                                     inspect: Duration)

/**
 * Configuration for docker client
 */
case class DockerClientConfig(parallelRuns: Int, timeouts: DockerClientTimeoutConfig, maskDockerRunArgs: Boolean)

/**
 * Serves as interface to the docker CLI tool.
 *
 * Be cautious with the ExecutionContext passed to this, as the
 * calls to the CLI are blocking.
 *
 * You only need one instance (and you shouldn't get more).
 */
class DockerClient(dockerHost: Option[String] = None,
                   config: DockerClientConfig = loadConfigOrThrow[DockerClientConfig](ConfigKeys.dockerClient))(
  executionContext: ExecutionContext)(implicit log: Logging, as: ActorSystem)
    extends DockerApi
    with ProcessRunner {
  implicit private val ec = executionContext

  // Determines how to run docker. Failure to find a Docker binary implies
  // a failure to initialize this instance of DockerClient.
  protected val dockerCmd: Seq[String] = {
    val alternatives = List("/usr/bin/docker", "/usr/local/bin/docker") ++ executableAlternatives

    val dockerBin = Try {
      alternatives.find(a => Files.isExecutable(Paths.get(a))).get
    } getOrElse {
      throw new FileNotFoundException(s"Couldn't locate docker binary (tried: ${alternatives.mkString(", ")}).")
    }

    val host = dockerHost.map(host => Seq("--host", s"tcp://$host")).getOrElse(Seq.empty[String])
    Seq(dockerBin) ++ host
  }

  protected def executableAlternatives: List[String] = List.empty

  // Invoke docker CLI to determine client version.
  // If the docker client version cannot be determined, an exception will be thrown and instance initialization will fail.
  // Rationale: if we cannot invoke `docker version` successfully, it is unlikely subsequent `docker` invocations will succeed.
  protected def getClientVersion(): String = {
    val vf = executeProcess(dockerCmd ++ Seq("version", "--format", "{{.Client.Version}}"), config.timeouts.version)
      .andThen {
        case Success(version) => log.info(this, s"Detected docker client version $version")
        case Failure(e) =>
          log.error(this, s"Failed to determine docker client version: ${e.getClass} - ${e.getMessage}")
      }
    Await.result(vf, 2 * config.timeouts.version)
  }
  val clientVersion: String = getClientVersion()

  protected val maxParallelRuns = config.parallelRuns
  protected val runSemaphore =
    new Semaphore( /* permits= */ if (maxParallelRuns > 0) maxParallelRuns else Int.MaxValue, /* fair= */ true)

  // Docker < 1.13.1 has a known problem: if more than 10 containers are created (docker run)
  // concurrently, there is a good chance that some of them will fail.
  // See https://github.com/moby/moby/issues/29369
  // Use a semaphore to make sure that at most 10 `docker run` commands are active
  // the same time.
  private def getNetworkName(args: Seq[String]): String = {
    args.sliding(2).collectFirst({ case Seq("--network", network) => network }).getOrElse("bridge")
  }
  private def getNetworkBandwidthAllocation(args: Seq[String]): String = {
    //network name follows s"${name}_network_${bandwidth}", from DockerContainerFactory.scala. Extract ONLY the "bandwidth" part of the string and return.
    val fullName = getNetworkName(args)
    val parts = fullName.split("_")
    if (parts.length < 3) {
      "10"
    } else {
      parts(parts.length - 1) //should be the bandwidth number
    }
  }

  private var networkCounter = 0
  def run(image: String, args: Seq[String] = Seq.empty[String])(
    implicit transid: TransactionId): Future[ContainerId] = {
    Future {
      blocking {
        // Acquires a permit from this semaphore, blocking until one is available, or the thread is interrupted.
        // Throws InterruptedException if the current thread is interrupted
        runSemaphore.acquire()
      }
    }.flatMap { _ =>
      // Iff the semaphore was acquired successfully

      //Create docker network
      // get network name
      val networkName = getNetworkName(args)
      val networkBandwidthAllocation = getNetworkBandwidthAllocation(args)
      val networkNameUnique = networkName ++ s"_${networkCounter}"
      var runArgs = args
      log.info(this,s"Parsed Network Name: ${networkName}")

      //run create_throttled_network.sh script with network name and rate limit from args
      val networkCreateFuture = executeProcess(
        Seq("bash", "create_throttled_network.sh", networkNameUnique, networkBandwidthAllocation, runArgs.mkString(" ")),
        config.timeouts.run
      ).flatMap( //if it succeeds return the network name
        _ => {
          log.info(this, s"Network created: ${networkNameUnique}")
          networkCounter += 1
          networkCounter = networkCounter % 1000 //just to keep the names from getting too long
          var runArgsStr = runArgs.mkString(" ")

          // find --network networkName and replace with --network networkNameUnique
          runArgsStr = runArgsStr.replaceAll(s"--network ${networkName}", s"--network ${networkNameUnique}")

          //turn it back into a sequence
          runArgs = runArgsStr.split(" ").toSeq
          log.info(this, s"New runArgs: ${runArgs.mkString(" ")}")

          Future[String](networkNameUnique)
        }
      ).recoverWith {
        case _ => {
          log.error(this, s"Failed to create network ${networkNameUnique}, using bridge network")
          var runArgsStr = runArgs.mkString(" ")

          // find --network networkName and replace with --network bridge
          runArgsStr = runArgsStr.replaceAll(s"--network ${networkName}", "--network bridge")

          //turn it back into a sequence
          runArgs = runArgsStr.split(" ").toSeq
          log.info(this, s"New runArgs: ${runArgs.mkString(" ")}")

          Future[String]("bridge")
        }
      }

      //TODO: add cleanup for any orphaned docker networks that might get created

      val containerCreationFuture = networkCreateFuture.flatMap({ //do whether or not it throws exception for rn
         _ => {
          runCmd(
            Seq("run", "-d") ++ runArgs ++ Seq(image),
            config.timeouts.run,
            if (config.maskDockerRunArgs) Some(Seq("run", "-d", "**ARGUMENTS sHIDDEN**", image)) else None)
        }
      })
      containerCreationFuture.andThen {
          // Release the semaphore as quick as possible regardless of the runCmd() result
        case _ => runSemaphore.release()
      }
      .map(ContainerId.apply)
      .recoverWith {
        // https://docs.docker.com/v1.12/engine/reference/run/#/exit-status
        // Exit status 125 means an error reported by the Docker daemon.
        // Examples:
        // - Unrecognized option specified
        // - Not enough disk space
        // Exit status 127 means an error that container command cannot be found.
        // Examples:
        // - executable file not found in $PATH": unknown
        case pre: ProcessUnsuccessfulException
            if pre.exitStatus == ExitStatus(125) || pre.exitStatus == ExitStatus(127) =>
          Future.failed(
            DockerContainerId
              .parse(pre.stdout)
              .map(BrokenDockerContainer(_, s"Broken container: ${pre.getMessage}", Some(pre.exitStatus.statusValue)))
              .getOrElse(pre))
      }
    }
  }

  def inspectIPAddress(id: ContainerId, network: String)(implicit transid: TransactionId): Future[ContainerAddress] =
    runCmd(
      Seq("inspect", "--format", s"{{.NetworkSettings.Networks.${network}.IPAddress}}", id.asString),
      config.timeouts.inspect).flatMap {
      case "<no value>" => Future.failed(new NoSuchElementException)
      case stdout       => Future.successful(ContainerAddress(stdout))
    }

  def pause(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("pause", id.asString), config.timeouts.pause).map(_ => ())

  def unpause(id: ContainerId)(implicit transid: TransactionId): Future[Unit] =
    runCmd(Seq("unpause", id.asString), config.timeouts.unpause).map(_ => ())

  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit] = {
    //TODO: Use docker inspect to find the network this is attached to, and if it is not "bridge" remove it when
    // removing the container

    //1. find network name
    //2. remove container
    //3. remove network if not bridge
    executeProcess(dockerCmd ++ Seq("inspect", id.asString, "--format", "{{.NetworkSettings.Networks}}"), config.timeouts.inspect)
      .flatMap { networks =>
        val networkName: String = networks.split("\\[")(1).split(":")(0)
        log.info(this, s"Network Name: ${networkName}")
        runCmd(Seq("rm", "-f", id.asString), config.timeouts.rm).flatMap { _ =>
          if (networkName != "bridge") {
            runCmd(Seq("network", "rm", networkName), config.timeouts.rm).andThen{
              case Success(_) => log.info(this, s"Network ${networkName} removed")
              case Failure(e) => log.error(this, s"Failed to remove network ${networkName}: ${e.getMessage}")
            }.map(_ => ())
          } else {
            Future.successful(())
          }
        }
      }
  }

  def ps(filters: Seq[(String, String)] = Seq.empty, all: Boolean = false)(
    implicit transid: TransactionId): Future[Seq[ContainerId]] = {
    val filterArgs = filters.flatMap { case (attr, value) => Seq("--filter", s"$attr=$value") }
    val allArg = if (all) Seq("--all") else Seq.empty[String]
    val cmd = Seq("ps", "--quiet", "--no-trunc") ++ allArg ++ filterArgs
    runCmd(cmd, config.timeouts.ps).map(_.linesIterator.toSeq.map(ContainerId.apply))
  }

  /**
   * Stores pulls that are currently being executed and collapses multiple
   * pulls into just one. After a pull is finished, the cached future is removed
   * to enable constant updates of an image without changing its tag.
   */
  private val pullsInFlight = TrieMap[String, Future[Unit]]()
  def pull(image: String)(implicit transid: TransactionId): Future[Unit] =
    pullsInFlight.getOrElseUpdate(image, {
      runCmd(Seq("pull", image), config.timeouts.pull).map(_ => ()).andThen { case _ => pullsInFlight.remove(image) }
    })

  def isOomKilled(id: ContainerId)(implicit transid: TransactionId): Future[Boolean] =
    runCmd(Seq("inspect", id.asString, "--format", "{{.State.OOMKilled}}"), config.timeouts.inspect).map(_.toBoolean)

  protected def runCmd(args: Seq[String], timeout: Duration, maskedArgs: Option[Seq[String]] = None)(
    implicit transid: TransactionId): Future[String] = {
    val cmd = dockerCmd ++ args
    log.info(this,cmd.mkString(" "))
//    val start = transid.started(
//      this,
//      LoggingMarkers.INVOKER_DOCKER_CMD(args.head),
//      s"running ${maskedArgs.map(maskedArgs => (dockerCmd ++ maskedArgs).mkString(" ")).getOrElse(cmd.mkString(" "))} (timeout: $timeout)",
//      logLevel = InfoLevel)
    log.info(this, s"running ${maskedArgs.map(maskedArgs => (dockerCmd ++ maskedArgs).mkString(" ")).getOrElse(cmd.mkString(" "))} (timeout: $timeout)")
    executeProcess(cmd, timeout).andThen {
//      case Success(success) => transid.finished(this, start, s"CMD RETURN: ${success}")
      case Success(success) => log.info(this, s"CMD RETURN: ${success}")
      case Failure(pte: ProcessTimeoutException) =>
//        transid.failed(this, start, pte.getMessage, ErrorLevel)
          log.error(this, s"CMD FAILURE: ${pte.getMessage}")
//      case Failure(t) => transid.failed(this, start, t.getMessage, ErrorLevel)
      case Failure(t) => log.error(this, s"CMD FAILURE: ${t.getMessage}")
    }
  }
}

trait DockerApi {

  /**
   * The version number of the docker client cli
   *
   * @return The version of the docker client cli being used by the invoker
   */
  def clientVersion: String

  /**
   * Spawns a container in detached mode.
   *
   * @param image the image to start the container with
   * @param args arguments for the docker run command
   * @return id of the started container
   */
  def run(image: String, args: Seq[String] = Seq.empty[String])(implicit transid: TransactionId): Future[ContainerId]

  /**
   * Gets the IP address of a given container.
   *
   * A container may have more than one network. The container has an
   * IP address in each of these networks such that the network name
   * is needed.
   *
   * @param id the id of the container to get the IP address from
   * @param network name of the network to get the IP address from
   * @return ip of the container
   */
  def inspectIPAddress(id: ContainerId, network: String)(implicit transid: TransactionId): Future[ContainerAddress]

  /**
   * Pauses the container with the given id.
   *
   * @param id the id of the container to pause
   * @return a Future completing according to the command's exit-code
   */
  def pause(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  /**
   * Unpauses the container with the given id.
   *
   * @param id the id of the container to unpause
   * @return a Future completing according to the command's exit-code
   */
  def unpause(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  /**
   * Removes the container with the given id.
   *
   * @param id the id of the container to remove
   * @return a Future completing according to the command's exit-code
   */
  def rm(id: ContainerId)(implicit transid: TransactionId): Future[Unit]

  /**
   * Returns a list of ContainerIds in the system.
   *
   * @param filters Filters to apply to the 'ps' command
   * @param all Whether or not to return stopped containers as well
   * @return A list of ContainerIds
   */
  def ps(filters: Seq[(String, String)] = Seq.empty, all: Boolean = false)(
    implicit transid: TransactionId): Future[Seq[ContainerId]]

  /**
   * Pulls the given image.
   *
   * @param image the image to pull
   * @return a Future completing once the pull is complete
   */
  def pull(image: String)(implicit transid: TransactionId): Future[Unit]

  /**
   * Determines whether the given container was killed due to
   * memory constraints.
   *
   * @param id the id of the container to check
   * @return a Future containing whether the container was killed or not
   */
  def isOomKilled(id: ContainerId)(implicit transid: TransactionId): Future[Boolean]
}

/** Indicates any error while starting a container that leaves a broken container behind that needs to be removed */
case class BrokenDockerContainer(id: ContainerId, msg: String, existStatus: Option[Int] = None) extends Exception(msg)
