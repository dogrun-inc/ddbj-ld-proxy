import { Client } from '@elastic/elasticsearch'

const client = new Client({
  // node: process.env.ELASTICSEARCH_HOST,
  //　compose.ymlから　"http://es01:9200"　を渡されていることを確認
  // しかしes01:9200に接続できていない雰囲気
  node: "http://192.168.11.20:9200"
})

// for DL metadata API get_metadata ~ get_annotation
const get_metadata = async function (ids, type="project") {
  const id_list = ids.split(",")
  const res = await client.search({
    "index": "bioproject",
    "body":  {
      "query": {
              "terms" : {
                "_id": id_list
              },
      }
    }
  })
  const data = res.hits.hits
  const metadatas = project_metadata(data);
  return metadatas
}
  
const project_metadata = function (results) {
  // 特定の属性のみを抽出
  const metadatas = results.map(result => {
    const metadata = {};
    metadata.identifier = result._source.identifier;
    metadata.title = result._source.title;
    metadata.description = result._source.description;
    metadata.organism = result._source.organism;
    metadata.organization = result._source.organization;

    // アノテーション処理
    const annotations = result._source._annotation;
    if (annotations) {
      // 属性リストに基づいたループ処理
      const props = [
        "sample_organism",
        "sample_taxid",
        "sample_host_organism",
        "sample_host_organism_id",
        "sample_host_disease",
        "sample_host_disease_id",
        "sample_host_location",
        "sample_ph_range",
        "sample_temperature_range",
      ];
      props.forEach(prop => {
        metadata[prop] = get_annotation(annotations, prop);
      });
    }
    return metadata;
  });
  return metadatas;
}
  
const get_annotation = function (annotations, property) {
  // 指定したプロパティのannotationを取得
  //const annotation = annotations.find(a => a.key === property)?.value;
  const annotation = annotations[property]
  if (annotation) {
      // 型によって処理を分岐
      if (Array.isArray(annotation)) {
      // リストの場合はカンマ区切り文字列に変換
      return annotation.join(",");
      } else if (typeof annotation === "object") {
      // オブジェクトの場合はJSON文字列に変換
      return JSON.stringify(annotation);
      } else {
      // その他の場合はそのまま返す
      return annotation;
      }
  } else {
      // annotationが存在しない場合はnullを返す
      return null;
  }
}
  
const dict2tsv = function (data) {
  if (typeof data[0] !== "object") {
    throw new Error("data[0] is not an object");
  } 
  
  const columnNames = Object.keys(data[0]);
  console.log(columnNames)
  
  //const csvWriterInstance = csvWriter.createObjectCsvWriter(csvWriterOptions);

  // 下記recordが変換されたtsvデータ
  let records = data.map(row => {
    const record = columnNames.map(n => row[n])
    console.log(record)
    return record.join("\t");
  });
  records.unshift(columnNames.join("\t"))
  return records.join("\n")
}
  
const helper = {
  get_metadata,
  dict2tsv,
}

export default helper
