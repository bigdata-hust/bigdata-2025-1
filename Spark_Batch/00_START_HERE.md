# 🎯 BẮT ĐẦU TỪ ĐÂY - START HERE!

> Hướng dẫn nhanh để hiểu và sử dụng dự án Yelp Big Data Analysis

---

## 📚 BẠN ĐANG TÌM GÌ?

### 🚀 "Tôi muốn chạy pipeline ngay!"
➜ Đọc: **README.md** (7KB - 5 phút)
- Quick start commands
- Test instructions
- Troubleshooting cơ bản

```bash
# Chạy ngay lệnh này:
python3 batch_main_v2.py --data-path ./data/
```

---

### 🏗️ "Tôi muốn hiểu cấu trúc dự án"
➜ Đọc: **PROJECT_STRUCTURE.md** (80KB - 30 phút)
- Chi tiết tất cả 13 files
- Hàm nào nằm ở đâu
- Mỗi module làm gì
- Ví dụ code cụ thể

**Nội dung:**
```
├─ Entry Points (batch_main.py, batch_main_v2.py)
├─ Core Modules (configuration, load_data, pipeline)
├─ Analytics Modules (7 basic + 2 advanced)
├─ UDF Library (7 UDFs)
├─ Testing (test suite)
└─ Data Flow & Dependencies
```

---

### 📐 "Tôi muốn xem sơ đồ trực quan"
➜ Đọc: **ARCHITECTURE_DIAGRAM.md** (35KB - 15 phút)
- Sơ đồ 3 tầng (Entry → Logic → Data)
- Data flow diagrams
- Class diagrams
- Module dependencies
- Function call sequences

**Sơ đồ gồm:**
```
┌─────────────┐
│ Entry Layer │ → batch_main.py, batch_main_v2.py
├─────────────┤
│ Logic Layer │ → pipeline, analytics, udf
├─────────────┤
│ Data Layer  │ → configuration, load_data
└─────────────┘
```

---

### 🧪 "Tôi muốn test các tính năng"
➜ Đọc: **LOCAL_TEST_GUIDE.md**
- Test từng feature riêng lẻ
- Expected outputs
- Troubleshooting chi tiết

```bash
# Test tất cả:
python3 test_local_features.py --test all
```

---

### 📋 "Tôi muốn biết implementation plan"
➜ Đọc: **IMPLEMENTATION_PLAN_PA1.md**
- Kế hoạch Phase 1 chi tiết
- Timeline & milestones
- Technical approach

---

## 🗺️ ROADMAP - Lộ trình đọc

### Người mới (Chưa biết gì về dự án)
```
1. README.md (5 min)              → Hiểu overview & quick start
2. ARCHITECTURE_DIAGRAM.md (15 min) → Xem sơ đồ trực quan
3. PROJECT_STRUCTURE.md (30 min)  → Đọc chi tiết từng module
4. Chạy test và pipeline          → Hands-on experience
```

### Developer (Muốn add features mới)
```
1. PROJECT_STRUCTURE.md            → Tìm module cần sửa
2. Đọc section "Function Mapping"  → Biết hàm nào ở đâu
3. Xem "Dependency Graph"          → Hiểu quan hệ modules
4. Add code theo pattern hiện có   → Maintain consistency
```

### Reviewer (Muốn review code)
```
1. ARCHITECTURE_DIAGRAM.md         → Hiểu high-level design
2. PROJECT_STRUCTURE.md            → Check implementation details
3. LOCAL_TEST_GUIDE.md             → Verify test coverage
```

---

## 🔍 TRA CỨU NHANH

### "Hàm X nằm ở file nào?"

| Cần tìm | File |
|---|---|
| Tạo Spark Session | `batch_configuration.py` → `SparkConfig.create_spark_session()` |
| Load dữ liệu | `batch_load_data.py` → `DataLoader.load_business_data()` |
| Analysis 1-7 | `batch_analytics.py` → `YelpAnalytics.analysis_X()` |
| Analysis 8-9 | `batch_analytics_advanced.py` → `AdvancedYelpAnalytics.analysis_X()` |
| UDF functions | `batch_udf.py` → 7 UDFs (3 Regular + 4 Pandas) |
| Pipeline orchestration | `batch_pipeline.py` → `YelpAnalysisPipeline` |
| Main entry V1 | `batch_main.py` → `main()` (7 analyses) |
| Main entry V2 | `batch_main_v2.py` → `main()` (9 analyses) |

### "Module X phụ thuộc vào module gì?"

```
batch_main_v2.py
  └─► batch_pipeline.py
        ├─► batch_configuration.py
        ├─► batch_load_data.py
        │     └─► batch_configuration.py
        └─► batch_analytics.py

batch_analytics_advanced.py
  └─► batch_udf.py
```

Chi tiết: Xem **ARCHITECTURE_DIAGRAM.md** → "Module Dependencies"

### "Tôi muốn thêm analysis mới?"

1. Tạo hàm static method trong `batch_analytics_advanced.py`:
   ```python
   @staticmethod
   def my_new_analysis(df1, df2, param1, param2):
       # Your logic here
       return result_df
   ```

2. Add vào pipeline trong `batch_main_v2.py`:
   ```python
   def run_analysis_10(self, param1, param2):
       result = AdvancedYelpAnalytics.my_new_analysis(
           self.review_df, self.business_df, param1, param2
       )
       self.results['my_analysis'] = result
       return result
   ```

3. Gọi trong `run_all_analyses_v2()`:
   ```python
   self.run_analysis_10(**config['analysis_10'])
   ```

Chi tiết: Xem **PROJECT_STRUCTURE.md** → "Adding New Analysis"

---

## 📊 STATISTICS - Con số

| Metric | Value |
|---|---|
| Tổng dòng code Python | 2,712 lines |
| Số Python files | 13 files |
| Số analyses | 9 (7 basic + 2 advanced) |
| Số UDFs | 7 (3 Regular + 4 Pandas UDF) |
| Số documentation files | 6 files (~150KB) |
| Test coverage | 4 feature tests |
| Spark features used | 15+ features |

---

## 💡 QUICK TIPS

### Chạy pipeline nhanh nhất
```bash
# V2 với tất cả tính năng
python3 batch_main_v2.py --data-path ./data/
```

### Test nhanh một feature
```bash
# Test chỉ UDF
python3 test_local_features.py --test udf
```

### Sử dụng một module riêng lẻ
```python
# Example: Chỉ chạy Analysis 8
from batch_configuration import SparkConfig
from batch_load_data import DataLoader
from batch_analytics_advanced import AdvancedYelpAnalytics

spark = SparkConfig.create_spark_session()
loader = DataLoader(spark, "./data/")
review_df = loader.load_review_data()
business_df = loader.load_business_data()

advanced = AdvancedYelpAnalytics()
result = advanced.trending_businesses(review_df, business_df)
result.show()
```

### Debug khi có lỗi
1. Check logs: Spark UI at http://localhost:4040
2. Verify data: `df.printSchema()`, `df.show(5)`
3. Check physical plan: `df.explain()`
4. Memory issues: Giảm memory trong `batch_configuration.py`

---

## ❓ FAQ - Câu hỏi thường gặp

**Q: Có bao nhiêu phiên bản pipeline?**
A: 2 phiên bản:
- V1 (`batch_main.py`): 7 analyses cơ bản
- V2 (`batch_main_v2.py`): 9 analyses với tính năng nâng cao

**Q: Nên dùng phiên bản nào?**
A: Dùng **V2** (`batch_main_v2.py`) để có đầy đủ tính năng nhất.

**Q: Làm sao biết code có chạy được không?**
A: Chạy test: `python3 test_local_features.py --test all`

**Q: File nào là entry point?**
A:
- `batch_main.py` (V1 - 7 analyses)
- `batch_main_v2.py` (V2 - 9 analyses) ⭐ Recommended

**Q: Tôi không có data thật, làm sao test?**
A: Chạy `python3 create_sample_data.py` để tạo sample data

**Q: UDF nào nhanh hơn?**
A: **Pandas UDF** nhanh hơn Regular UDF 10-100x. Xem `batch_udf.py`

**Q: Tại sao có broadcast join?**
A: Optimize performance khi join với bảng nhỏ. Xem Analysis 1, 3, 4, 6 trong `batch_analytics.py`

**Q: Window Functions được dùng ở đâu?**
A: Analysis 8 (`batch_analytics_advanced.py`) - Trending Businesses

**Q: Pivot/Unpivot ở đâu?**
A: Analysis 9 (`batch_analytics_advanced.py`) - Category Performance Matrix

---

## 📁 FILE INDEX - Danh mục files

### 📖 Documentation (Tài liệu)
```
00_START_HERE.md            ← BẠN ĐANG Ở ĐÂY!
README.md                   ← Quick Start (ĐỌC ĐẦU TIÊN!)
PROJECT_STRUCTURE.md        ← Cấu trúc chi tiết (ĐỌC THỨ HAI!)
ARCHITECTURE_DIAGRAM.md     ← Sơ đồ trực quan (ĐỌC THỨ BA!)
LOCAL_TEST_GUIDE.md         ← Hướng dẫn test
IMPLEMENTATION_PLAN_PA1.md  ← Kế hoạch Phase 1
QUICKSTART.md              ← Quick start guide (older)
README_BATCH.md            ← Batch mode readme (older)
```

### 🎯 Entry Points (Điểm khởi chạy)
```
batch_main.py              ← V1: 7 analyses cơ bản
batch_main_v2.py           ← V2: 9 analyses nâng cao ⭐
```

### 🔧 Core Modules (Module cốt lõi)
```
batch_configuration.py     ← Spark config & schemas
batch_load_data.py         ← Load dữ liệu từ JSON
batch_pipeline.py          ← Pipeline orchestrator
```

### 📊 Analytics Modules (Module phân tích)
```
batch_analytics.py         ← 7 analyses cơ bản
batch_analytics_advanced.py ← 2 analyses nâng cao ⭐
```

### 🎯 UDF Library (Thư viện hàm)
```
batch_udf.py               ← 7 UDFs (3 Regular + 4 Pandas) ⭐
```

### 🧪 Testing & Utilities (Test & tiện ích)
```
test_local_features.py     ← Test suite ⭐
create_sample_data.py      ← Tạo sample data
```

⭐ = Files mới trong Phase 1

---

## 🎓 HỌC THEO CHỦ ĐỀ

### Chủ đề: "Spark Configuration"
➜ File: `batch_configuration.py`
➜ Đọc thêm: **PROJECT_STRUCTURE.md** → Section "batch_configuration.py"

### Chủ đề: "Window Functions"
➜ File: `batch_analytics_advanced.py` → Analysis 8
➜ Đọc thêm: **PROJECT_STRUCTURE.md** → Section "Analysis 8"

### Chủ đề: "Pandas UDF vs Regular UDF"
➜ File: `batch_udf.py`
➜ Đọc thêm: **PROJECT_STRUCTURE.md** → Section "UDF Library"
➜ Test: `python3 test_local_features.py --test udf`

### Chủ đề: "Broadcast Join Optimization"
➜ Files: `batch_analytics.py` (analyses 1, 3, 4, 6)
➜ Đọc thêm: **PROJECT_STRUCTURE.md** → Section "Broadcast Join"
➜ Test: `python3 test_local_features.py --test broadcast`

### Chủ đề: "Pivot/Unpivot Operations"
➜ File: `batch_analytics_advanced.py` → Analysis 9
➜ Đọc thêm: **PROJECT_STRUCTURE.md** → Section "Analysis 9"
➜ Test: `python3 test_local_features.py --test pivot`

---

## 🚀 BƯỚC TIẾP THEO

### 1. Đọc documentation theo thứ tự:
- [ ] README.md (5 phút)
- [ ] ARCHITECTURE_DIAGRAM.md (15 phút)
- [ ] PROJECT_STRUCTURE.md (30 phút)

### 2. Hands-on:
- [ ] Chạy test: `python3 test_local_features.py --test all`
- [ ] Chạy pipeline: `python3 batch_main_v2.py --data-path ./data/`
- [ ] Xem kết quả: `ls -lh output_v2/`

### 3. Explore code:
- [ ] Đọc `batch_udf.py` - Hiểu cách viết UDF
- [ ] Đọc `batch_analytics_advanced.py` - Hiểu Window Functions & Pivot
- [ ] Đọc `batch_pipeline.py` - Hiểu pipeline flow

### 4. Customize:
- [ ] Thử thêm một analysis mới
- [ ] Thử tạo một UDF mới
- [ ] Thử modify configuration

---

## 📞 CẦN HỖ TRỢ?

### Không biết bắt đầu từ đâu?
➜ Đọc **README.md** trước!

### Muốn hiểu cấu trúc code?
➜ Đọc **PROJECT_STRUCTURE.md**

### Muốn xem visual diagrams?
➜ Đọc **ARCHITECTURE_DIAGRAM.md**

### Gặp lỗi khi test?
➜ Đọc **LOCAL_TEST_GUIDE.md** → Section "Troubleshooting"

### Muốn hiểu implementation approach?
➜ Đọc **IMPLEMENTATION_PLAN_PA1.md**

---

## ✨ KEY HIGHLIGHTS

### Dự án này có gì đặc biệt?

1. **📚 Documentation hoàn chỉnh**: 6 files (~150KB) giải thích chi tiết
2. **🏗️ Kiến trúc rõ ràng**: 3 tầng (Entry → Logic → Data)
3. **🎯 Modularity cao**: Mỗi module có thể dùng độc lập
4. **⚡ Performance optimized**: Broadcast Join, Pandas UDF, Caching
5. **🧪 Test coverage tốt**: Test suite cho 4 tính năng nâng cao
6. **📊 9 analyses**: 7 cơ bản + 2 nâng cao (Window, Pivot)
7. **🎨 7 UDFs**: 3 Regular + 4 Pandas UDF (10-100x nhanh hơn)

---

## 🎯 TÓM TẮT 3 DÒNG

1. **Quick Start**: Đọc `README.md` → Chạy `python3 batch_main_v2.py --data-path ./data/`
2. **Hiểu cấu trúc**: Đọc `PROJECT_STRUCTURE.md` + `ARCHITECTURE_DIAGRAM.md`
3. **Test features**: Chạy `python3 test_local_features.py --test all`

---

**Chúc bạn khám phá dự án thành công! 🎉**

---

*Last Updated: 2025-12-16*
*Version: 2.0 (Advanced Features)*
