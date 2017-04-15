package com.techmonad.pipeline.logging


import org.slf4j.{Logger, LoggerFactory}


trait Logging {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass())

  protected def debug(msg: => String): Unit = logger.debug(msg)

  protected def debug(msg: => String, exception: Throwable): Unit = logger.debug(msg, exception)

  protected def info(msg: => String, exception: Throwable): Unit = logger.info(msg, exception)

  protected def info(msg: => String): Unit = logger.info(msg)

  protected def warn(msg: => String): Unit = logger.warn(msg)

  protected def warn(msg: => String, exception: Throwable): Unit = logger.warn(msg, exception)

  protected def error(msg: => String): Unit = logger.error(msg)

  protected def error(msg: => String, exception: Throwable): Unit = logger.error(msg, exception)

}
