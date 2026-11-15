# Which taxi type was the most used in the first half of 2024?

ผมวัด most used ด้วยจำนวน valid trip records โดยหนึ่งแถวแทนหนึ่ง trip record
เกณฑ์ valid คือมี pickup และ dropoff, dropoff ไม่ก่อน pickup และ pickup อยู่ในเดือนของไฟล์

ช่วงข้อมูล: 2024-01 ถึง 2024-06
ประเภทบริการ: yellow, green, fhv, fhvhv

บริการที่มีจำนวนเที่ยวสูงสุดคือ **High Volume FHV (FHVHV)** จำนวน **120,864,668 เที่ยว** ต่อประเภท

| ประเภทบริการ | Valid trips | สัดส่วน |
|---|---:|---:|
| High Volume FHV (FHVHV) | 120,864,668 | 80.76% |
| Yellow Taxi | 20,331,644 | 13.59% |
| For-Hire Vehicle (FHV) | 8,119,228 | 5.43% |
| Green Taxi | 339,763 | 0.23% |

เมื่อแยกดูรายเดือน High Volume FHV (FHVHV) ยังมากที่สุดทุกเดือน โดยมีสัดส่วน 79.7% ถึง 82.0% ของ valid trips ในเดือนนั้น

รวม valid trips: 149,655,303 เที่ยว
Raw records: 149,655,796 แถว; rejected: 493 แถว
ผลเทียบ baseline S1: MATCH

ยอดตาม borough ใช้ pickup borough และรวมค่าที่หา borough ไม่ได้ไว้ใน Unknown
ผมใช้ valid trips ทั้งหมดในการคำนวณ ส่วน sample เป็นตัวอย่างจากแต่ละ batch สำหรับตรวจ schema
ที่มาและผลตรวจอยู่ใน monthly_trip_counts.csv, quality_report.csv และ verify_run.json
