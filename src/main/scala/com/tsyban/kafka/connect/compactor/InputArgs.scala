package com.tsyban.kafka.connect.compactor

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class InputArgs(processingDate: LocalDate,
                     dataSet: List[String],
                     configPath: Option[String])

object InputArgs {

  val inputDateFormat = "yyyy-MM-dd"
  var inputDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(inputDateFormat)
  val parseLocalDate: String => LocalDate = LocalDate.parse(_, inputDateFormatter)

  def apply(input: String*): InputArgs = {
    val lifted = input.lift
    new InputArgs(lifted(0).map(parseLocalDate).getOrElse(LocalDate.now()),
                  lifted(1).map(_.split(",")).getOrElse(Array()).toList,
                  lifted(2))
  }

}

