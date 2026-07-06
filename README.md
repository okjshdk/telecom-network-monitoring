# Data Engineer Project | End-to-end Telecom Network Monitoring Pipeline (GCP)

## Điểm nổi bật

- Thiết kế và triển khai pipeline ETL real-time trên Google Cloud sử dụng Apache Beam và Dataflow để xử lý dữ liệu sự kiện mạng viễn thông.
- Xây dựng kiến trúc Lakehouse (Bronze–Silver–Gold) với Cloud Storage và BigQuery, hỗ trợ dữ liệu từ thô đến sẵn sàng phân tích.
- Xử lý streaming từ Pub/Sub theo cửa sổ 60 giây, đảm bảo phân tích gần thời gian thực.
- Thực hiện làm sạch và kiểm tra chất lượng dữ liệu (loại bỏ giá trị thiếu, sai lệch và ngoài ngưỡng).
- Tạo các đặc trưng phân tích như chất lượng mạng, độ trễ, thông lượng và giờ cao điểm.
- Tổng hợp KPI hiệu suất giúp giám sát trạm phát sóng phục vụ dashboard và báo cáo vận hành.
- Tối ưu pipeline bằng windowing và xử lý streaming trên Dataflow, lưu dữ liệu vào BigQuery cho phân tích SQL.

## 1. Tổng quan
  Dự án này xây dựng 1 pipeline end-to-end để thu thập, xử lý và lưu trữ dữ liệu game events \
  từ các trò chơi online theo giời gian thực vào data lake.

## 2. Kiến trúc hệ thống
<img width="1636" height="654" alt="system-architecture" src="https://github.com/okjshdk/telecom-network-monitoring/blob/main/architecture.png" />



## 3. Công nghệ sử dụng
  - Storage: Cloud Storage (GCS)
  - Streaming/Message Queue: Pub/Sub
  - Processing & Orchestration: DataFlow + Apache Beam
  - Dashboard: Data Studio
  - Infrastructure: GCP

## 4. Use Case
  ### Telecom Tower Performance Dashboard
  - Giám sát hiệu năng của từng trạm BTS thông qua các KPI chính.
  - Phát hiện sớm các trạm có dấu hiệu hoạt động bất thường.
  - Hỗ trợ đội vận hành theo dõi chất lượng mạng.
  ### Network Performance Trends Dashboard
  - Theo dõi xu hướng hiệu năng mạng theo thời gian.
  - So sánh chất lượng mạng giữa giờ cao điểm và giờ bình thường.
  - Hỗ trợ đánh giá và tối ưu hiệu suất mạng.

