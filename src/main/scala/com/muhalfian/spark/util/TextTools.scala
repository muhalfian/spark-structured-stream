package com.muhalfian.spark.util

import jsastrawi.morphology.DefaultLemmatizer

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable.{Set, HashSet}
import org.apache.spark.sql.functions.udf

object TextTools {

  // =================== LOAD DICTIONARY SASTRAWI ==============

  val dictionary : Set[String] = HashSet[String]()
  val fileStream = getClass.getResourceAsStream("/kata-dasar.txt")
  for (line <- Source.fromInputStream(fileStream).getLines) {
    dictionary.add(line)
  }
  var lemmatizer = new DefaultLemmatizer(dictionary.asJava);

  // ============= DICTIONARY STOPWORD SASTRAWI =================
  // https://github.com/har07/PySastrawi/blob/master/src/Sastrawi/StopWordRemover/StopWordRemoverFactory.py

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

  // =================== REGEX TOKENIZER CONFIGURATION ====================

  val regexTokenizer = new RegexTokenizer()
    .setInputCol("raw_text")
    .setOutputCol("text_regex")
    .setPattern("\\d*\\W+")
    .setMinTokenLength(2)

  // =================== STOPWORD CONFIGURATION =========================

  val remover = new StopWordsRemover()
    .setStopWords(stopwordsArr)
    .setInputCol("text_regex")
    .setOutputCol("text_preprocess")

  // ======================= STEMMING UDF ===============================

  val stemming = udf ((words: Seq[String]) => {

    val stemmed = words
                  .map(_.replaceAll("[^A-Za-z]", ""))     // replace all non - char
                  .map(_.trim).filter(_ != "")            // remove null list
                  .map(row => lemmatizer.lemmatize(row))  // stemming using sastrawi
  })

  val stringify = udf((word: String) => {
    word.replaceAll("[\\]\\[]", "")
        .replaceAll("\"", " ")
  })

}
