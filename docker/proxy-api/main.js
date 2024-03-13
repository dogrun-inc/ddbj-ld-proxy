import Fastify from 'fastify'
import { Client } from '@elastic/elasticsearch'
import fastifyCors from '@fastify/cors'

import fs from 'fs';
import archiver from 'archiver';
import csvWriter from 'csv-writer';

const fastify = Fastify({
  logger: process.env.LOGGER === 'on' ? true : false,
})

fastify.register(fastifyCors)

const client = new Client({
  // node: process.env.ELASTICSEARCH_HOST,
  //　compose.ymlから　"http://es01:9200"　を渡されていることを確認
  // しかしes01:9200に接続できていない雰囲気
  node: "http://192.168.11.20:9200"
})

// 以下API-ES接続テスト用の関数
fastify.get('/test', async (req) => {
  req.log.info(JSON.stringify(req.query))
  const res = await client.search({
    "index": "bioproject",
    "body": {
      "size": 2,
      "query": {
        "match_all": {}
      }
    }
})
  return {
    hits: res.hits.hits
  }
})



// メタデータが取得できているか確認するためのAPI
fastify.get('/dl/test/:ids', async(req, res) => {
  const ids = "PRJNA13696,PRJNA13699,PRJNA13700,PRJNA13702,PRJNA13729,PRJNA18537,PRJNA18833,PRJNA18929";
  let metadatas = await get_metadata(ids, "project")
  res.send(metadatas)
})

// for DL metadata API get_metadata ~ get_annotation
async function get_metadata(ids, type="project") {
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

function project_metadata(results) {
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

function get_annotation(annotations, property) {
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



function dict2csv(data) {
  const header = data[0].keys();
  const csvWriterOptions = {
    delimiter: '\t',
    header,
  };

  //const csvWriterInstance = csvWriter.createObjectCsvWriter(csvWriterOptions);

  // 下記recordが変換されたarrayデータ
  const records = data.map(row => {
    const record = {};
    for (const key of header) {
      record[key] = row[key];
    }
    return record;
  });

  return records

}


// ここまでDL API開発用コード（引き続き使うかも）



fastify.get('/', async (req) => {
  req.log.info(JSON.stringify(req.query))

  const test_str = process.env.ELASTICSEARCH_HOST + "/bioproject/_doc/PRJNA16522"
  if (!req.query.q) {
    return { hits: [] }
  }

  const q = req.query.q.toLowerCase()

  const res = await client.search({
    "index": "bioproject",
    "body": {
      "size": 10,
      "query": {
        "bool": {
          "should": [
            {
              "wildcard": {
                "id": {
                  "value": `*${q}*`
                }
              }
            },
            {
              "wildcard": {
                "label": {
                  "value": `*${q}*`
                }
              }
            },
          ],
          "minimum_should_match": 1
        }
      }
    }
})
  return {
    hits: res.hits.hits
  }
})

fastify.get('/bioproject/_doc/:id', async (req, reply) => {
  if (!req.params.id) {
    return { }
  }
  let id = req.params.id
  const index = await client.get({
    "index": "bioproject",
    "id": id
  })

  return {
    index
  }
})

fastify.get('/bioproject/_search', async (req, reply) => {
  if (!req.query.q) {
    return { hits: [] }
  }
  const q = req.query.q.toLowerCase()
  const res = await client.search({
    "index": "bioproject",
    "q": q
  })

  return res
})

fastify.post('/bioproject', async (req, reply) => {
  const res = await client.search({
    "index": "bioproject",
    "body": req.body
  })

  return res
})

// Copies　of the above apis as bioproject~

fastify.get('/project/_doc/:id', async (req, reply) => {
  if (!req.params.id) {
    return { }
  }
  let id = req.params.id
  const index = await client.get({
    "index": "project",
    "id": id
  })

  return {
    index
  }
})

fastify.get('/project/_search', async (req, reply) => {
  if (!req.query.q) {
    return { hits: [] }
  }
  const q = req.query.q.toLowerCase()
  const res = await client.search({
    "index": "project",
    "q": q
  })

  return res
})

fastify.post('/project', async (req, reply) => {
  const res = await client.search({
    "index": "project",
    "body": req.body
  })

  return res
})


fastify.get('/genome/_doc/:id', async(req, reply) => {
  if (!req.params.id) {
    return { }
  }
  let id = req.params.id
  const index = await client.get({
    "index": "genome",
    "id": id
  })
  return index
})

fastify.get('/genome/_search', async(req, reply) => {
  if (!req.query.q) {
    return { hits: [] }
  }
  const q = req.query.q.toLowerCase()
  const res = await client.search({
    "index": "genome",
    "q": q
  })
  return res
})

fastify.post('/genome', async(req, reply) => {
  const res = await client.search({
    "index": "genome",
    "body": req.body
  })

  return res
})

fastify.get('/plotly_data', async (req) => {
    const view_id = req.query.view;
    const sample_id = req.query.id;

    if (view_id != ""){
      // TODO: viewを指定し対応する処理の結果を返す
    }

    let sample_list = sample_id.split(',');
    // name(あるいは_id)が sample_listに含まれるレコードを選択する
    const res = await client.search({
        "index": 'taxonomic_comparison',
        "body": {
                "query": {
                        "terms" : {
                            "_id": sample_list
                        },
            },
            "size": 1000
        }
    })
    let res_tmp = res.hits.hits
    // ESのレスポンスが引数の順番と限らないためsample_listを再取得
    if (res_tmp.length){
      sample_list = res_tmp.map(spl => {
        return spl._source.taxonomic_comparison.name
      })
      // [{taxon: , value: },,]の配列をサンプルごと取得
      let tax_value = res_tmp.map(spl => {
        return spl._source.taxonomic_comparison.composition
      })
      // taxonoごとの雛形のobjectを作成する
      let res_taxonomic_comparison = tax_value[0].map(taxon => {
        return {x: sample_list, y: [], name: taxon.taxon, type: "bar"}
      })
  
      res_taxonomic_comparison.forEach((element, index) => {
        // サンプル毎各taxonの値をyにpushする
        tax_value.forEach(tax => {
          element.y.push(tax[index].value)
        })
      })
  
      return res_taxonomic_comparison
    } else {
      return []
    }

})

fastify.get('/metastanza_data/bioproject/:id', async (req) => {
  if (!req.params.id) {
    return {}
  }else{
    const id = req.params.id.toUpperCase()
    //const view = req.query.view.toLowerCase()
    // idを引数に検索結果をhash_table用にフォーマットして返す
    const index = await client.get({
      "index": "bioproject",
      "id": id
    })

    return {identifier: index._source.identifier, 
      organism: index._source.organism,
      title: index._source.title,
      description: index._source.description,
      organisazion: index._source.organization,
      created: index._source.dateCreated,
      modified: index._source.dateModified
    }
  }
})

fastify.get('/metastanza_data/bioproject', async (req) => {
  if (!req.query.q) {
    return { hits: [] }
  }else{
    const q = req.query.q.toLowerCase()
    const res = await client.search({
      "index": "bioproject",
      "q": q
    })

    let jsn = res.hits.hits.map(h => {
      return {
          identifier: h._source.identifier,
          organism: h._source.organism,
          title: h._source.title,
          created: h._source.dateCreated,
          modified: h._source.dateModified
      }
    })

    return jsn
  }
})


fastify.get('/metastanza_data/:index_name/:id', async (req) => {
  if (!req.params.index_name || !req.params.id) {
    return {}
  }else{
    const q = req.query.q.toLowerCase()
    const res = await client.search({
      "index": "bioproject",
      "q": q
    })

    let jsn = res.hits.hits.map(h => {
      return {
          // 全てのk:vをマップ
      }
    })

    return jsn
  }
})


fastify.get('/project/metadata/:ids', async (req, rep) => {
  if (!req.params.ids) {
    rep
      .code(400)
      .type('text/plain')
      .send('Bad Request. (no id set.)')
  }
  const data = await get_metadata(req.params.ids)
  rep.header('Content-Disposition', 'attachment; filename=project_metadata.json')
  rep.send(data)
})


fastify.get('/sequence/genome/:ids', async (req, rep) => {
  if (!req.params.ids) {
    rep
      .code(400)
      .type('text/plain')
      .send('Bad Request. (no id set.)')
  }

  // TODO: ids から pathMap を取得するメソッドを作成
  //const pathMap = getSequencePathList(req.params.id)
  const pathMap = new Map()
  const pathList = [
    '/mnt/data/mdatahub_sample/c0/ref16s_500k.fasta',
    '/mnt/data/mdatahub_sample/c1/ref16s_500k.fasta',
    '/mnt/data/mdatahub_sample/c2/ref16s_500k.fasta',
  ]
  req.params.ids.sqlit(',').forEach((id, index) => {
    pathMap.set(id, pathList[index])
  })

  // TODO: 一時ディレクトリは暫定（決まったら変更）
  const tempDir = '/mnt/data/tmp'

  const timestamp = Date.now().toString()
  const zipFilePath = tempDir + `/${timestamp}.zip`
  const output = fs.createWriteStream(zipFilePath)
  const archive = archiver('zip')
  archive.pipe(output)

  pathMap.forEach((v, k) => {
    if (v === '') {
      return
    }
    const fileName = v.split('/').at(-1)
    archive.file(v, { name: `${k}/${fileName}` })
  })
  archive.finalize()

  output.on('close', () => {
    rep.type('application/zip')
    rep.send(fs.createReadStream(zipFilePath))
  })
})


const start = async () => {
  try {
    await fastify.listen(process.env.PORT, '0.0.0.0')
  } catch (e) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
