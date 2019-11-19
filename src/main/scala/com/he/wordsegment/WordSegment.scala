package com.he.wordsegment

import java.util

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.domain.Result
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.{BaseAnalysis, IndexAnalysis, NlpAnalysis, ToAnalysis}
import org.apdplat.word.WordSegmenter
import org.apdplat.word.segmentation.SegmentationAlgorithm

object WordSegment {
  def main(args: Array[String]): Unit = {

//    ansj()
//    word()
//    jieba()

  }

  /**
    * ansj
    */
  def ansj(): Unit ={
    //分词准备
    val stop = new StopRecognition()
    stop.insertStopNatures("w")//过滤掉标点
    //    stop.insertStopNatures("m")//过滤掉数词
    stop.insertStopNatures("u")//过滤掉助词
    stop.insertStopNatures("d")//过滤掉副词
    stop.insertStopNatures("a")//过滤掉形容词
    stop.insertStopNatures("c")//过滤掉连词
    stop.insertStopNatures("null")//过滤null词性
    stop.insertStopNatures("<br />")//过滤<br　/>词性
    stop.insertStopNatures(":")
    stop.insertStopNatures("'")

    val res: Result = BaseAnalysis.parse("中国人名识别是中国人民的一个骄傲.孙健人民在CSDN中学到了很多最早iteye是java学习笔记叫javaeye但是java123只是一部分")
    //    println(res.toStringWithOutNature(","))
    val res1: Result = ToAnalysis.parse("柏林墙守卫案与“于欢案”中正义精神之比较《》")
    //    println(res1.toStringWithOutNature(","))
    val res2: Result = NlpAnalysis.parse("论高职制造类专业人才培养要素优化———基于“中国制造2025”视域 金属3D 打印技术探究")
    //    println(res2.toStringWithOutNature(","))
    val res3: Result = IndexAnalysis.parse("中国人名识别是中国人民的一个骄傲.孙健人民在CSDN中学到了很多最早iteye是java学习笔记叫javaeye但是java123只是一部分")
    val str = res2.recognition(stop).toStringWithOutNature(",")
    //    println(str)

    //    关键词提取
    val key = new KeyWordComputer(100)
    val content = " 苏菲玛索df.m f.sdf.sdfsdf没收到免费送免费"
    val keywords = key.computeArticleTfidf(content)
    //    println(keywords)
  }

  /**
    * word
    */
    def word(): Unit ={
      val content = ""
      //    关键词提取
      val words = WordSegmenter.segWithStopWords(content,SegmentationAlgorithm.BidirectionalMaximumMatching)
      val words1 = WordSegmenter.segWithStopWords(content,SegmentationAlgorithm.BidirectionalMinimumMatching) //双向最小匹配
      println(words)

    }

  /**
    * jieba
    */
    def jieba(): Unit ={
      import scala.collection.JavaConverters._
      val jiebaSegmenter = new JiebaSegmenter()
      val strings = jiebaSegmenter.process("杨尚川是APDPlat应用级产品开发平台的作者",SegMode.INDEX)
      val tokens: util.List[SegToken] = jiebaSegmenter.process("杨尚川是APDPlat应用级产品开发平台的作者",SegMode.SEARCH)
      println(strings)
      tokens.asScala.foreach(x => {
        print(x.word+" ")
      })
    }

}
