package com.gmall.realtime.util

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 实现对象属性拷贝
 */
object MyBeanUtils {

  /**
   * 将srcObj中的属性的值拷贝到destObj对应的属性上
   */
  def copyProperties(SrcObj: AnyRef, destObj: AnyRef): Unit = {
    if (SrcObj == null || destObj == null) return
    //          获取srcObj中所有的属性
    val srcfields: Array[Field] = SrcObj.getClass.getDeclaredFields

    for (srcfield <- srcfields) {
    Breaks.breakable{
      //getMethonName
      val getMethonName: String = srcfield.getName
      //      getMethonName
      val setMethodName: String = srcfield.getName + "_$eq"

      //      从srcObj中获取get方法对象
      val getMethod: Method = SrcObj.getClass.getDeclaredMethod(getMethonName)
      //      从destbj中获取set方法对象
      val setMethod: Method =
      try {
      destObj.getClass.getDeclaredMethod(setMethodName, srcfield.getType)
      } catch {
        //            NoSuchMethodExeception
        case  exception: Exception => Breaks.break()
      }
      // 忽略val属性
      val destField: Field = destObj.getClass.getDeclaredField(srcfield.getName)
      if(Modifier.FINAL.equals(destField.getModifiers)){
        Breaks.break()
      }
//调用个体方法获取到srcObj属性的值，再调用set方法将获取到的值赋给destObj属性
      setMethod.invoke(destObj,getMethod.invoke(SrcObj))
    }

    }
  }
}
