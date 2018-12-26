package com.muhalfian.spark

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{explode, split}

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}

import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.collection.mutable.ArrayBuffer

import jsastrawi.morphology.{Lemmatizer, DefaultLemmatizer}
import scala.collection.mutable.{Set, HashSet}
// import java.io.BufferedReader
// import java.io.InputStreamReader
import scala.io.Source
import collection.JavaConverters._
// import java.util.{Set, HashSet}


object MediaStream extends StreamUtils {

    val kafkaHost = "ubuntu"
    val kafkaPort = "9092"
    val topic = "online_media"
    val startingOffsets = "earliest"
    val kafkaBroker = kafkaHost+":"+kafkaPort

    val schema : StructType = StructType(Seq(
        StructField("link", StringType,true),
        StructField("source", StringType, true),
        StructField("authors", StringType, true),
        StructField("image", StringType, true),
        StructField("publish_date", StringType, true),
        StructField("title", StringType, true),
        StructField("text", StringType, true)
      )
    )

    val myschema = Seq(
        "link",
        "source",
        "authors",
        "image",
        "publish_date",
        "title",
        "text"
    )

    val dictionary : Set[String] = HashSet[String]()

    // load dictionary stemming sastrawi
    val filename = "/home/blade1/Documents/spark-structured-stream/src/main/scala/com/muhalfian/spark/data/kata-dasar.txt"
    for (line <- Source.fromFile(filename).getLines) {
        dictionary.add(line)
    }
    val dict : java.util.Set[String] = dictionary.asJava
    var lemmatizer = new DefaultLemmatizer(dict);

    // dictionary stopwords sastrawi
    val stopwordsArr = Array("a","ada","adalah","adanya","adapun","agak","agaknya","agar","akan","akankah","akhir",
            "akhiri","akhirnya","aku","akulah","amat","amatlah","anda","andalah","antar","antara",
            "antaranya","apa","apaan","apabila","apakah","apalagi","apatah","arti","artinya","asal",
            "asalkan","atas","atau","ataukah","ataupun","awal","awalnya","b","bagai","bagaikan",
            "bagaimana","bagaimanakah","bagaimanapun","bagainamakah","bagi","bagian","bahkan","bahwa",
            "bahwasannya","bahwasanya","baik","baiklah","bakal","bakalan","balik","banyak","bapak",
            "baru","bawah","beberapa","begini","beginian","beginikah","beginilah","begitu","begitukah",
            "begitulah","begitupun","bekerja","belakang","belakangan","belum","belumlah","benar",
            "benarkah","benarlah","berada","berakhir","berakhirlah","berakhirnya","berapa","berapakah",
            "berapalah","berapapun","berarti","berawal","berbagai","berdatangan","beri","berikan",
            "berikut","berikutnya","berjumlah","berkali-kali","berkata","berkehendak","berkeinginan",
            "berkenaan","berlainan","berlalu","berlangsung","berlebihan","bermacam","bermacam-macam",
            "bermaksud","bermula","bersama","bersama-sama","bersiap","bersiap-siap","bertanya",
            "bertanya-tanya","berturut","berturut-turut","bertutur","berujar","berupa","besar",
            "betul","betulkah","biasa","biasanya","bila","bilakah","bisa","bisakah","boleh","bolehkah",
            "bolehlah","buat","bukan","bukankah","bukanlah","bukannya","bulan","bung","c","cara",
            "caranya","cukup","cukupkah","cukuplah","cuma","d","dahulu","dalam","dan","dapat","dari",
            "daripada","datang","dekat","demi","demikian","demikianlah","dengan","depan","di","dia",
            "diakhiri","diakhirinya","dialah","diantara","diantaranya","diberi","diberikan","diberikannya",
            "dibuat","dibuatnya","didapat","didatangkan","digunakan","diibaratkan","diibaratkannya",
            "diingat","diingatkan","diinginkan","dijawab","dijelaskan","dijelaskannya","dikarenakan",
            "dikatakan","dikatakannya","dikerjakan","diketahui","diketahuinya","dikira","dilakukan",
            "dilalui","dilihat","dimaksud","dimaksudkan","dimaksudkannya","dimaksudnya","diminta",
            "dimintai","dimisalkan","dimulai","dimulailah","dimulainya","dimungkinkan","dini","dipastikan",
            "diperbuat","diperbuatnya","dipergunakan","diperkirakan","diperlihatkan","diperlukan",
            "diperlukannya","dipersoalkan","dipertanyakan","dipunyai","diri","dirinya","disampaikan",
            "disebut","disebutkan","disebutkannya","disini","disinilah","ditambahkan","ditandaskan",
            "ditanya","ditanyai","ditanyakan","ditegaskan","ditujukan","ditunjuk","ditunjuki","ditunjukkan",
            "ditunjukkannya","ditunjuknya","dituturkan","dituturkannya","diucapkan","diucapkannya",
            "diungkapkan","dong","dua","dulu","e","empat","enak","enggak","enggaknya","entah","entahlah",
            "f","g","guna","gunakan","h","hadap","hai","hal","halo","hallo","hampir","hanya","hanyalah",
            "hari","harus","haruslah","harusnya","helo","hello","hendak","hendaklah","hendaknya","hingga",
            "i","ia","ialah","ibarat","ibaratkan","ibaratnya","ibu","ikut","ingat","ingat-ingat","ingin",
            "inginkah","inginkan","ini","inikah","inilah","itu","itukah","itulah","j","jadi","jadilah",
            "jadinya","jangan","jangankan","janganlah","jauh","jawab","jawaban","jawabnya","jelas",
            "jelaskan","jelaslah","jelasnya","jika","jikalau","juga","jumlah","jumlahnya","justru",
            "k","kadar","kala","kalau","kalaulah","kalaupun","kali","kalian","kami","kamilah","kamu",
            "kamulah","kan","kapan","kapankah","kapanpun","karena","karenanya","kasus","kata","katakan",
            "katakanlah","katanya","ke","keadaan","kebetulan","kecil","kedua","keduanya","keinginan",
            "kelamaan","kelihatan","kelihatannya","kelima","keluar","kembali","kemudian","kemungkinan",
            "kemungkinannya","kena","kenapa","kepada","kepadanya","kerja","kesampaian","keseluruhan",
            "keseluruhannya","keterlaluan","ketika","khusus","khususnya","kini","kinilah","kira",
            "kira-kira","kiranya","kita","kitalah","kok","kurang","l","lagi","lagian","lah","lain",
            "lainnya","laku","lalu","lama","lamanya","langsung","lanjut","lanjutnya","lebih","lewat",
            "lihat","lima","luar","m","macam","maka","makanya","makin","maksud","malah","malahan",
            "mampu","mampukah","mana","manakala","manalagi","masa","masalah","masalahnya","masih",
            "masihkah","masing","masing-masing","masuk","mata","mau","maupun","melainkan","melakukan",
            "melalui","melihat","melihatnya","memang","memastikan","memberi","memberikan","membuat",
            "memerlukan","memihak","meminta","memintakan","memisalkan","memperbuat","mempergunakan",
            "memperkirakan","memperlihatkan","mempersiapkan","mempersoalkan","mempertanyakan","mempunyai",
            "memulai","memungkinkan","menaiki","menambahkan","menandaskan","menanti","menanti-nanti",
            "menantikan","menanya","menanyai","menanyakan","mendapat","mendapatkan","mendatang","mendatangi",
            "mendatangkan","menegaskan","mengakhiri","mengapa","mengatakan","mengatakannya","mengenai",
            "mengerjakan","mengetahui","menggunakan","menghendaki","mengibaratkan","mengibaratkannya",
            "mengingat","mengingatkan","menginginkan","mengira","mengucapkan","mengucapkannya","mengungkapkan",
            "menjadi","menjawab","menjelaskan","menuju","menunjuk","menunjuki","menunjukkan","menunjuknya",
            "menurut","menuturkan","menyampaikan","menyangkut","menyatakan","menyebutkan","menyeluruh",
            "menyiapkan","merasa","mereka","merekalah","merupakan","meski","meskipun","meyakini","meyakinkan",
            "minta","mirip","misal","misalkan","misalnya","mohon","mula","mulai","mulailah","mulanya","mungkin",
            "mungkinkah","n","nah","naik","namun","nanti","nantinya","nya","nyaris","nyata","nyatanya",
            "o","oleh","olehnya","orang","p","pada","padahal","padanya","pak","paling","panjang","pantas",
            "para","pasti","pastilah","penting","pentingnya","per","percuma","perlu","perlukah","perlunya",
            "pernah","persoalan","pertama","pertama-tama","pertanyaan","pertanyakan","pihak","pihaknya",
            "pukul","pula","pun","punya","q","r","rasa","rasanya","rupa","rupanya","s","saat","saatnya","saja",
            "sajalah","salam","saling","sama","sama-sama","sambil","sampai","sampai-sampai","sampaikan","sana",
            "sangat","sangatlah","sangkut","satu","saya","sayalah","se","sebab","sebabnya","sebagai",
            "sebagaimana","sebagainya","sebagian","sebaik","sebaik-baiknya","sebaiknya","sebaliknya",
            "sebanyak","sebegini","sebegitu","sebelum","sebelumnya","sebenarnya","seberapa","sebesar",
            "sebetulnya","sebisanya","sebuah","sebut","sebutlah","sebutnya","secara","secukupnya","sedang",
            "sedangkan","sedemikian","sedikit","sedikitnya","seenaknya","segala","segalanya","segera",
            "seharusnya","sehingga","seingat","sejak","sejauh","sejenak","sejumlah","sekadar","sekadarnya",
            "sekali","sekali-kali","sekalian","sekaligus","sekalipun","sekarang","sekaranglah","sekecil",
            "seketika","sekiranya","sekitar","sekitarnya","sekurang-kurangnya","sekurangnya","sela","selain",
            "selaku","selalu","selama","selama-lamanya","selamanya","selanjutnya","seluruh","seluruhnya",
            "semacam","semakin","semampu","semampunya","semasa","semasih","semata","semata-mata","semaunya",
            "sementara","semisal","semisalnya","sempat","semua","semuanya","semula","sendiri","sendirian",
            "sendirinya","seolah","seolah-olah","seorang","sepanjang","sepantasnya","sepantasnyalah",
            "seperlunya","seperti","sepertinya","sepihak","sering","seringnya","serta","serupa","sesaat",
            "sesama","sesampai","sesegera","sesekali","seseorang","sesuatu","sesuatunya","sesudah",
            "sesudahnya","setelah","setempat","setengah","seterusnya","setiap","setiba","setibanya",
            "setidak-tidaknya","setidaknya","setinggi","seusai","sewaktu","siap","siapa","siapakah",
            "siapapun","sini","sinilah","soal","soalnya","suatu","sudah","sudahkah","sudahlah","supaya",
            "t","tadi","tadinya","tahu","tak","tambah","tambahnya","tampak","tampaknya","tandas","tandasnya",
            "tanpa","tanya","tanyakan","tanyanya","tapi","tegas","tegasnya","telah","tempat","tentang","tentu",
            "tentulah","tentunya","tepat","terakhir","terasa","terbanyak","terdahulu","terdapat","terdiri",
            "terhadap","terhadapnya","teringat","teringat-ingat","terjadi","terjadilah","terjadinya","terkira",
            "terlalu","terlebih","terlihat","termasuk","ternyata","tersampaikan","tersebut","tersebutlah",
            "tertentu","tertuju","terus","terutama","tetap","tetapi","tiap","tiba","tiba-tiba","tidak",
            "tidakkah","tidaklah","tiga","toh","tuju","tunjuk","turut","tutur","tuturnya","u","ucap","ucapnya",
            "ujar","ujarnya","umumnya","ungkap","ungkapnya","untuk","usah","usai","v","w","waduh","wah","wahai",
            "waktunya","walau","walaupun","wong","x","y","ya","yaitu","yakin","yakni","yang","z")

    def main(args: Array[String]): Unit = {

        val spark = getSparkSession(args)
        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        // ======================== READ STREAM ================================

        // read data stream from Kafka
        val kafka = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",kafkaBroker)
            .option("subscribe", topic)
            .option("startingOffsets", startingOffsets)
            .load()

        // Transform data stream to Dataframe
        val kafkaDF = kafka.selectExpr("CAST(value AS STRING)").as[(String)]
            .select(from_json($"value", schema).as("data"))
            .select("data.*")

        // ==================== PREPROCESS APACHE LUCENE =======================

        // val preprocess = udf((content: String) => {
        //     val analyzer=new IndonesianAnalyzer()
        //     val tokenStream=analyzer.tokenStream("contents", content)
        //     val term=tokenStream.addAttribute(classOf[CharTermAttribute]) //CharTermAttribute is what we"re extracting
        //
        //     tokenStream.reset() // must be called by the consumer before consumption to clean the stream
        //
        //     // var result = ArrayBuffer.empty[String]
        //     var result = ""
        //
        //     while(tokenStream.incrementToken()) {
        //         val termValue = term.toString
        //         if (!(termValue matches ".*[\\d\\.].*")) {
        //             result += term.toString + " "
        //         }.show(false)
        //     }
        //     tokenStream.end()
        //     tokenStream.close()
        //     result
        // })

        // val preprocessDF = kafkaDF
        //     .withColumn("text_preprocess", preprocess(col("text").cast("string")))

        // ===================== PREPROCESS SASTRAWI ===========================


        // val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("text_preprocess")

        val regexTokenizer = new RegexTokenizer()
          .setInputCol("text")
          .setOutputCol("text_regex")
          .setPattern("\\W\\d*") // alternatively .setPattern("\\w+").setGaps(false)
        val regexDF = regexTokenizer.transform(kafkaDF)

        val remover = new StopWordsRemover()
            .setStopWords(stopwordsArr)
            .setInputCol("text_regex")
            .setOutputCol("text_filter")
        val filteredDF = remover.transform(regexDF)

        val stemming = udf ((words: String) => {
            var filtered = words.replaceAll(",[]", " ");
            var word = filtered.split(" ")
              .toSeq
              .map(_.trim)
              .filter(_ != "")
            var hasil = ArrayBuffer.empty[String]
            // var hasil = ""

            word.foreach{ row =>
                var stemmed = lemmatizer.lemmatize(row)
                hasil += stemmed + " "
            }
            hasil
        })

        val preprocessDF = filteredDF.select("text, text_preprocess")
            .withColumn("text_preprocess", stemming(col("text_filter").cast("string")))


        // ======================== AGGREGATION ================================

        // // Aggregate User Defined Function
        // val aggregate = udf((content: Column) => {
        //     val splits = explode(split(content, " "))
        //     println(splits)
        // })
        //
        // // Aggregate Running in DF
        // val aggregateDF = preprocessDF
        //     .withColumn("text_preprocess", aggregate(col("text_preprocess")))

        // =========================== SINK ====================================

        //Show Data after processed
        preprocessDF.writeStream
            .format("console")
            .option("truncate","false")
            .start()
            .awaitTermination()
    }



}
