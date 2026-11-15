# Data contract

## Grain และความหมาย

Trusted มีหนึ่งแถวต่อ valid trip record จาก source หนึ่ง batch ผมคงนิยาม valid ของ S1 และไม่ deduplicate ด้วยการคาดเดา Trip ID

เวลา pickup/dropoff คงค่าจาก source โดยไม่แปลง timezone และ cast เป็น DuckDB `TIMESTAMP` ที่ความละเอียด microsecond ส่วนเวลาของ logs และ markers ใช้ UTC

## Trusted schema

| Column | DuckDB type | Nullable | ความหมาย |
|---|---|---|---|
| service_type | VARCHAR | ไม่ได้ | yellow, green, fhv หรือ fhvhv |
| pickup_datetime | TIMESTAMP | ไม่ได้ | เวลารับจาก column ของ service นั้น |
| dropoff_datetime | TIMESTAMP | ไม่ได้ | เวลาส่ง โดยไม่ก่อน pickup |
| trip_duration_min | DOUBLE | ไม่ได้ | `epoch(dropoff - pickup) / 60.0` |
| pu_location_id | INTEGER | ได้ | pickup ID หลัง cast; ค่าไม่เป็นจำนวนเต็ม/เกินช่วง cast เป็น null |
| do_location_id | INTEGER | ได้ | dropoff ID หลัง cast |
| pu_borough | VARCHAR | ได้ | LEFT JOIN pickup ID กับ lookup |
| do_borough | VARCHAR | ได้ | LEFT JOIN dropoff ID กับ lookup |
| dq_long_trip | BOOLEAN | ไม่ได้ | duration มากกว่า threshold ใน config |
| dq_zero_duration | BOOLEAN | ไม่ได้ | pickup เท่ากับ dropoff |
| dq_unknown_location | BOOLEAN | ไม่ได้ | ID ฝั่งใดฝั่งหนึ่งเป็น null หรือไม่อยู่ใน lookup |
| batch_id | VARCHAR | ไม่ได้ | service และเดือนของ source file |
| source_file | VARCHAR | ไม่ได้ | ชื่อ source Parquet |
| run_id | VARCHAR | ไม่ได้ | run ที่เขียน partition นี้ |

Pickup ต้องอยู่ในช่วงต้นเดือนแบบ inclusive ถึงต้นเดือนถัดไปแบบ exclusive การ join ไม่กรอง unknown locations และ lookup ต้องมี LocationID ที่เป็นจำนวนเต็มบวก ไม่ซ้ำ และมี Borough จึงจะใช้ได้

`run_id` ใน trusted เป็นรอบที่เขียน partition ส่วน `run_id` ใน `verify_run.json` เป็นรอบที่วิเคราะห์ window ล่าสุด จึงต่างกันได้เมื่อใช้ข้อมูลที่ประมวลผลแล้ว

## Source mappings

| Service | Pickup | Dropoff | PU / DO |
|---|---|---|---|
| yellow | tpep_pickup_datetime | tpep_dropoff_datetime | PULocationID / DOLocationID |
| green | lpep_pickup_datetime | lpep_dropoff_datetime | PULocationID / DOLocationID |
| fhv | pickup_datetime | dropOff_datetime | PUlocationID / DOlocationID |
| fhvhv | pickup_datetime | dropoff_datetime | PULocationID / DOLocationID |

FHV Location IDs เป็น BIGINT ใน source ที่ใช้ ผม cast เป็น INTEGER ตาม contract และเก็บปัญหา location เป็น warning ไม่เปลี่ยนนิยาม valid

## Reject, warning และ BLOCK

`reject_reason` มีเหตุผลหลักหนึ่งค่าต่อแถว ตามลำดับ `missing_pickup`, `missing_dropoff`, `dropoff_before_pickup`, `pickup_outside_batch_month` แถว rejected อยู่ใน `data/quarantine/rows/batch_id=.../rejected.parquet` พร้อมข้อมูลมาตรฐานและ lineage; source เต็มยังอยู่ใน raw

Warning นับเฉพาะ valid rows และ flags ซ้อนกันได้ `warn_rows` นับแถวที่มีอย่างน้อยหนึ่ง flag ส่วน `outside_month_rows` นับปัญหาเดือนโดยอิสระจากเหตุผล reject หลัก

BLOCK ใช้เมื่ออ่านไม่ได้/ไม่มีแถว/ขาด columns ที่ต้องใช้, เวลาไม่ใช่ timestamp แบบไม่มี timezone, แถวนอกเดือนมากกว่า 1% หรือ valid ratio น้อยกว่า 99% รายงานทั้ง batch อยู่ที่ `data/quarantine/batches/{batch_id}.json` หากยังจำแนกแถวไม่ได้จะเก็บ counts เป็น null แทนการอ้างว่าเป็นศูนย์

## Curated schema

| ตาราง | Grain | Columns |
|---|---|---|
| monthly_trip_counts | service–month | service_type VARCHAR, pickup_month VARCHAR, number_of_trips BIGINT |
| trips_by_borough | service–month–pickup borough | service_type VARCHAR, pickup_month VARCHAR, pu_borough VARCHAR, number_of_trips BIGINT |

`pu_borough` ที่เป็น null รวมเป็น `Unknown` เพื่อให้ยอดรวมเท่ากับ monthly counts ส่วน `dq_unknown_location` ตรวจทั้ง pickup และ dropoff จึงใช้คนละมุมกับ pickup borough summary

## Storage และการยอมรับข้อมูล

หนึ่ง batch มี trusted file เดียวที่ `data/trusted/trips/service_type=.../pickup_month=.../part-0.parquet` เขียนผ่าน staging แล้ว replace ไฟล์เดิม

VerifyBatch ตรวจ schema ตามลำดับ columns, row count เท่ากับ valid rows, required fields ไม่มี null, เดือนตรง batch, เวลา/duration/lineage ถูกต้อง และ borough/flags ตรงกับ lookup ก่อนสร้าง `_SUCCESS`

Marker เก็บ source SHA, reference SHA, processing fingerprint, trusted SHA, counts, run ID และผล verification ถ้า fingerprint หรือไฟล์เปลี่ยนจะผ่าน gate ใหม่ `--force` บังคับประมวลผล batch ที่เลือกใหม่ได้

Quarantine และรายงานหลักเป็นสถานะล่าสุดของ batch/window ประวัติแต่ละ run เก็บใน batch history และ snapshots ของ evidence
