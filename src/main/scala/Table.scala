import util.Util.Row
import util.Util.Line
import TestTables.{table3, tableFunctional, tableImperative, tableObjectOriented}

import scala.annotation.tailrec

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName) match {
      case Some(value) => Some(predicate(value))
      case None => None
    }
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    f1.eval(r) match {
      case Some(value1) => f2.eval(r) match {
        case Some(value2) => Some(value1 && value2)
        case None => None
      }
      case None => None
    }
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    f1.eval(r) match {
      case Some(value1) => f2.eval(r) match {
        case Some(value2) => Some(value1 || value2)
        case None => None
      }
      case None => None
    }
  }

}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = target.eval match {
    case None => None
    case Some(t) => t.select(columns)
  }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = target.eval match {
    case None => None
    case Some(t) => t.filter(condition)
  }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = target.eval match {
    case None => None
    case Some(t) => Some(t.newCol(name, defaultVal))
  }
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = t1.eval match {
    case None => None
    case Some(table1) => t2.eval match {
      case None => None
      case Some(table2) => table1.merge(key, table2)
    }
  }
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String =
    (columnNames :: tabular).map(_.mkString(",")).mkString("\n")

  private def transpose(table: List[Line]): List[Line] = {
    def getFirstCol(table: List[Line]): Line = table.map(_.head)

    def removeFirstCol(table: List[Line]): List[Line] = table.map(_.tail)

    @tailrec
    def transposeAux(acc: List[Line], battery: List[Line]): List[Line] =
      battery.head match {
        case Nil => acc
        case _ => transposeAux(acc ++ List(getFirstCol(battery)), removeFirstCol(battery))
      }

    transposeAux(Nil, table)
  }

  def getColumns: List[Line] = transpose(columnNames :: tabular)

  // 2.1
  def select(columns: Line): Option[Table] =
    if (!columns.forall(this.columnNames.contains(_))) None
    else {
      val selectedColsMap = this.getColumns
                                .filter(line => columns.contains(line.head))
                                .map(line => line.head -> line)
                                .toMap

      val transposed = transpose(columns.map(selectedColsMap(_)))

      Some(new Table(transposed.head, transposed.tail))
    }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    def op1(row: Row): Option[Line] =
      cond.eval(row) match {
        case Some(value) => {
          if (value) Some(columnNames.map(row(_)))
          else None
        }
        case None => None
      }

    def op2(optLine: Option[Line]): Boolean =
      optLine match {
        case Some(_) => true
        case None => false
      }

    this.tabular.map(columnNames.zip(_).toMap)
                .map(op1)
                .filter(op2)
                .map(_.get) match {
                  case Nil => None
                  case lines => Some(new Table(columnNames, lines))
                }
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table =
    new Table(columnNames ++ List(name), tabular.map(_ ++ List(defaultVal)))

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    def completeRow(row: Row, colNames: List[String]): Row =
      colNames.map(colName => colName -> row.getOrElse(colName, "")).toMap

    def updateRow(row1: Row, row2: Row): Row =
      row1.toList.map(pair => {
        val newValue = row2(pair._1)
        if (pair._2 == "") pair._1 -> newValue
        else if (newValue == "" || pair._2 == newValue) pair
        else pair._1 -> (pair._2 + ";" + newValue)
      }).toMap

    def keymapToTabular(keymap: Map[String, Map[String, String]], cols: List[String]): List[Line] =
      (for (entry <- keymap) yield cols.foldRight(Nil: Line)((col, acc) => entry._2.getOrElse(col, "") :: acc)).toList

    if (!this.columnNames.contains(key)) None
    else if (!other.getColumnNames.contains(key)) None
    else {
      val newColumnNames = other.getColumnNames
        .foldRight(Nil: List[String])((name, acc) => if (this.columnNames.contains(name)) acc else name :: acc)

      val colNames = this.columnNames ++ newColumnNames

      val keyMap1 = this.tabular.map(this.columnNames.zip(_).toMap)
                                .map(row => row(key) -> completeRow(row, colNames))
                                .toMap

      val keyMap2 = other.getTabular
                         .map(other.getColumnNames.zip(_).toMap)
                         .map(completeRow(_, colNames))
                         .map(row => keyMap1.get(row(key)) match {
                           case None => row(key) -> row
                           case Some(value1) => row(key) -> updateRow(value1, row)
                         })
                         .toMap

      Some(new Table(colNames, keymapToTabular(keyMap1 ++ keyMap2, colNames)))
    }
  }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val tokenized = s.split("\n").toList.map(_.split(",", -1).toList)

    new Table(tokenized.head, tokenized.tail)
  }
}