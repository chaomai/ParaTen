package org.chaomai.paratd.util

/**
  * Created by chaomai on 14/05/2017.
  */
import org.slf4j.Logger

class LazyLogger(log: Logger) extends Serializable {
  def info(msg: => String): Unit = {
    if (log.isInfoEnabled) log.info(msg)
  }

  def debug(msg: => String): Unit = {
    if (log.isDebugEnabled) log.debug(msg)
  }

  def trace(msg: => String): Unit = {
    if (log.isTraceEnabled) log.trace(msg)
  }

  def warn(msg: => String): Unit = {
    if (log.isWarnEnabled) log.warn(msg)
  }

  def error(msg: => String): Unit = {
    if (log.isErrorEnabled) log.error(msg)
  }
}
