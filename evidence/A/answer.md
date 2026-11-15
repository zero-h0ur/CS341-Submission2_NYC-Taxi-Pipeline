# ผลวิเคราะห์ช่วง 2024-01 ถึง 2024-03

ผมวัด most used ด้วยจำนวน valid trip records โดยหนึ่งแถวแทนหนึ่ง trip record
เกณฑ์ valid คือมี pickup และ dropoff, dropoff ไม่ก่อน pickup และ pickup อยู่ในเดือนของไฟล์

ช่วงข้อมูล: 2024-01 ถึง 2024-03
ประเภทบริการ: yellow, green, fhv, fhvhv

บริการที่มีจำนวนเที่ยวสูงสุดคือ **High Volume FHV (FHVHV)** จำนวน **60,303,866 เที่ยว** ต่อประเภท

| ประเภทบริการ | Valid trips | สัดส่วน |
|---|---:|---:|
| High Volume FHV (FHVHV) | 60,303,866 | 81.53% |
| Yellow Taxi | 9,554,542 | 12.92% |
| For-Hire Vehicle (FHV) | 3,935,561 | 5.32% |
| Green Taxi | 167,567 | 0.23% |

รวม valid trips: 73,961,536 เที่ยว
Raw records: 73,961,790 แถว; rejected: 254 แถว
ผลเทียบ baseline S1: NOT_APPLICABLE

ยอดตาม borough ใช้ pickup borough และรวมค่าที่หา borough ไม่ได้ไว้ใน Unknown
ผมใช้ valid trips ทั้งหมดในการคำนวณ ส่วน sample เป็นตัวอย่างจากแต่ละ batch สำหรับตรวจ schema
ที่มาและผลตรวจอยู่ใน monthly_trip_counts.csv, quality_report.csv และ verify_run.json
