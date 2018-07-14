/** 
* build a parsing query that extract the face detail we need from JSON and make it easily accessible
**/

WITH face_data AS (
	SELECT
	  ts,
	  source,
	  cast(json_extract(data, '$.facedetails') as array<json>) as arr
	FROM "default"."demoanalyticsapp_table"
	WHERE type = 'face'
),
faces AS (
	SELECT
	 ts,
	 source, 
	 cast(json_extract(f, '$.agerange') as map<varchar, integer>) as agerange,
	 cast(json_extract(f, '$.smile') as map<varchar, varchar>) as smile,
	 cast(json_extract(f, '$.eyeglasses') as map<varchar, varchar>) as eyeglasses,
	 cast(json_extract(f, '$.sunglasses') as map<varchar, varchar>) as sunglasses,
	 cast(json_extract(f, '$.gender') as map<varchar, varchar>) as gender,
	 cast(json_extract(f, '$.beard') as map<varchar, varchar>) as beard,
	 cast(json_extract(f, '$.mustache') as map<varchar, varchar>) as mustache,
	 cast(json_extract(f, '$.eyesopen') as map<varchar, varchar>) as eyesopen,
	 cast(json_extract(f, '$.mouthopen') as map<varchar, varchar>) as mouthopen,
	 cast(json_extract(f, '$.emotions') as array<map<varchar, varchar>>) as emotions
	FROM face_data
	CROSS JOIN UNNEST(arr) AS t(f)
)
SELECT *
FROM faces

/**
* build a parsing query that extract the text info we need from JSON and make it easily accessible
**/

WITH text_data AS (
	SELECT
      ts,
      source,
      transform (
        filter (
          cast(json_extract(data, '$.textdetections') as array<json>),
          x -> cast(json_extract_scalar(x, '$.confidence') as double) > 75.00 and json_extract_scalar(x, '$.type') = 'WORD'
        ),
        x -> cast(row(json_extract_scalar(x, '$.detectedtext'), json_extract_scalar(x, '$.type')) as row(text varchar, type varchar))
      ) as text_detected
	FROM "default"."demoanalyticsapp_table"
	WHERE type = 'text'
)
select * from text_data


/**
* Putting it all together to extract text and face info, join them and produce the file table.
**/

WITH face_data AS (
	SELECT
	  ts,
	  source,
	  cast(json_extract(data, '$.facedetails') as array<json>) as arr
	FROM "default"."demoanalyticsapp_table"
	WHERE type = 'face'
),
faces AS (
	SELECT
	 ts,
	 source, 
	 cast(json_extract(f, '$.agerange') as map<varchar, integer>) as agerange,
	 cast(json_extract(f, '$.smile') as map<varchar, varchar>) as smile,
	 cast(json_extract(f, '$.eyeglasses') as map<varchar, varchar>) as eyeglasses,
	 cast(json_extract(f, '$.sunglasses') as map<varchar, varchar>) as sunglasses,
	 cast(json_extract(f, '$.gender') as map<varchar, varchar>) as gender,
	 cast(json_extract(f, '$.beard') as map<varchar, varchar>) as beard,
	 cast(json_extract(f, '$.mustache') as map<varchar, varchar>) as mustache,
	 cast(json_extract(f, '$.eyesopen') as map<varchar, varchar>) as eyesopen,
	 cast(json_extract(f, '$.mouthopen') as map<varchar, varchar>) as mouthopen,
	 cast(json_extract(f, '$.emotions') as array<map<varchar, varchar>>) as emotions
	FROM face_data
	CROSS JOIN UNNEST(arr) AS t(f)
),
text_data AS (
	SELECT
      ts,
      source,
      transform (
        filter (
          cast(json_extract(data, '$.textdetections') as array<json>),
          x -> cast(json_extract_scalar(x, '$.confidence') as double) > 75.00 and json_extract_scalar(x, '$.type') = 'WORD'
        ),
        x -> cast(row(json_extract_scalar(x, '$.detectedtext'), json_extract_scalar(x, '$.type')) as row(text varchar, type varchar))
      ) as text_detected
	FROM "default"."demoanalyticsapp_table"
	WHERE type = 'text'
)
SELECT a.*, b.text_detected
FROM faces a
JOIN text_data b on a.source = b.source


/**
* Find all the participants within a specific age range that are wearing eye glasses
* "faces" is a view created from the above parsing query
**/

SELECT count(*)
FROM "analyticsapp"."final_table"
WHERE 
  agerange['high'] <=40 AND agerange['low'] >= 20
  AND eyeglasses['value'] = 'true' AND CAST(eyeglasses['confidence'] AS double) > 75.00
