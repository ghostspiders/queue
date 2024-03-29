/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.queue.utils

/**
 * Some common constants
 */
object Time {
  val NsPerUs = 1000
  val UsPerMs = 1000
  val MsPerSec = 1000
  val NsPerMs = NsPerUs * UsPerMs
  val NsPerSec = NsPerMs * MsPerSec
  val UsPerSec = UsPerMs * MsPerSec
  val SecsPerMin = 60
  val MinsPerHour = 60
  val HoursPerDay = 24
  val SecsPerHour = SecsPerMin * MinsPerHour
  val SecsPerDay = SecsPerHour * HoursPerDay
  val MinsPerDay = MinsPerHour * HoursPerDay
}

/**
 * A mockable interface for time functions
 */
trait Time {
  
  def milliseconds: Long

  def nanoseconds: Long

  def sleep(ms: Long)
}

/**
 * The normal system implementation of time functions
 */
object SystemTime extends Time {
  
  def milliseconds: Long = System.currentTimeMillis
  
  def nanoseconds: Long = System.nanoTime
  
  def sleep(ms: Long): Unit = Thread.sleep(ms)
  
}
