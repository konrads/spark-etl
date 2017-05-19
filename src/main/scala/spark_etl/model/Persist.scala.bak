package spark_etl.model

import net.jcazevedo.moultingyaml._
import org.apache.spark.storage.StorageLevel

sealed trait Persist { def asSpark: StorageLevel }

object Persist {
  object NONE extends Persist { def asSpark = StorageLevel.NONE }
  object DISK_ONLY extends Persist { def asSpark = StorageLevel.DISK_ONLY }
  object DISK_ONLY_2 extends Persist { def asSpark = StorageLevel.DISK_ONLY_2 }
  object MEMORY_ONLY extends Persist { def asSpark = StorageLevel.MEMORY_ONLY }
  object MEMORY_ONLY_2 extends Persist { def asSpark = StorageLevel.MEMORY_ONLY_2 }
  object MEMORY_ONLY_SER extends Persist { def asSpark = StorageLevel.MEMORY_ONLY_SER }
  object MEMORY_ONLY_SER_2 extends Persist { def asSpark = StorageLevel.MEMORY_ONLY_SER_2 }
  object MEMORY_AND_DISK extends Persist { def asSpark = StorageLevel.MEMORY_AND_DISK }
  object MEMORY_AND_DISK_2 extends Persist { def asSpark = StorageLevel.MEMORY_AND_DISK_2 }
  object MEMORY_AND_DISK_SER extends Persist { def asSpark = StorageLevel.MEMORY_AND_DISK_SER }
  object MEMORY_AND_DISK_SER_2 extends Persist { def asSpark = StorageLevel.MEMORY_AND_DISK_SER_2 }
  object OFF_HEAP extends Persist { def asSpark = StorageLevel.OFF_HEAP }

  implicit val typeFormat = new YamlFormat[Persist] {
    def read(value: YamlValue): Persist = value match {
      case YamlString(x) =>
        x.toUpperCase match {
          case "NONE" => NONE
          case "DISK_ONLY" => DISK_ONLY
          case "DISK_ONLY_2" => DISK_ONLY_2
          case "MEMORY_ONLY" => MEMORY_ONLY
          case "MEMORY_ONLY_2" => MEMORY_ONLY_2
          case "MEMORY_ONLY_SER" => MEMORY_ONLY_SER
          case "MEMORY_ONLY_SER_2" => MEMORY_ONLY_SER_2
          case "MEMORY_AND_DISK" => MEMORY_AND_DISK
          case "MEMORY_AND_DISK_2" => MEMORY_AND_DISK_2
          case "MEMORY_AND_DISK_SER" => MEMORY_AND_DISK_SER
          case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
          case "OFF_HEAP" => OFF_HEAP
        }
      case _ => deserializationError("Invalid Persist mode, see Spark StorageLevel options")
    }
    def write(g: Persist) = ???
  }
}
