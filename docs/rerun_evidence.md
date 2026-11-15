# หลักฐานการรัน A–D

ผมเริ่ม A ด้วย --force เพื่อสร้าง trusted ของมกราคม–มีนาคมใหม่ รวม batch ที่ทดสอบไว้ก่อนหน้าแล้ว
B ขยายถึงมิถุนายน ส่วน C รันช่วงเดิมซ้ำ และ D ลบเฉพาะ marker ของ green_2024-03 ก่อนรันใหม่

| รอบ | ช่วงเดือน | PUBLISHED | ALREADY_PROCESSED | วินาที | สถานะ | หลักฐาน |
|---|---|---:|---:|---:|---|---|
| A | 2024-01 ถึง 2024-03 | 12 | 0 | 25.663 | SUCCESS | [summary](../evidence/A/run_summary.json), [log](../evidence/A/run.log), [verification](../evidence/A/verify_run.json) |
| B | 2024-01 ถึง 2024-06 | 12 | 12 | 29.018 | SUCCESS | [summary](../evidence/B/run_summary.json), [log](../evidence/B/run.log), [verification](../evidence/B/verify_run.json) |
| C | 2024-01 ถึง 2024-06 | 0 | 24 | 9.181 | SUCCESS | [summary](../evidence/C/run_summary.json), [log](../evidence/C/run.log), [verification](../evidence/C/verify_run.json) |
| D | 2024-01 ถึง 2024-06 | 1 | 23 | 9.376 | SUCCESS | [summary](../evidence/D/run_summary.json), [log](../evidence/D/run.log), [verification](../evidence/D/verify_run.json) |

D มีจำนวนแถวของ partition ก่อนลบ marker 57,447 แถว และหลังประมวลผลใหม่ 57,447 แถว
[จำนวนแถวก่อนและหลัง](../evidence/D_marker_loss.json)

SHA-256 ของ monthly_trip_counts.csv, trips_by_borough.csv และ service_summary.csv ตรงกันใน B, C และ D
การเปรียบเทียบนี้ใช้ตารางวิเคราะห์ ส่วน run ID และ metadata การประมวลผลเปลี่ยนได้

เวลาที่รายงานเป็นผลวัดบนเครื่องผม โดย raw ถูกดาวน์โหลดไว้แล้ว จึงไม่ได้ใช้สรุปว่าเร็วกว่า S1 บน Colab
