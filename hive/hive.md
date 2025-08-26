# 常用函数

1. SUBSTRING()
2. substring_index(str, 分隔符, count )
3. regexp_extract(string, pattern, extract_index)
   1. **string**：需要被匹配的原始字符串
   2. **pattern**：正则表达式模式
   3. **extract_index**：提取的组索引（0 表示整个匹配，1 表示第一个括号内的组，依此类推）
4. from_unixtime(unix_timestamp [, format])
   1. **unix_timestamp**：必需参数，表示 Unix 时间戳（长整型数值）
   2. **format**：可选参数，指定输出日期时间的格式（字符串），默认格式（'yyyy-MM-dd HH:mm:ss'）
5. from_utc_timestamp(utc_timestamp, timezone)
   1. **utc_timestamp**：必需参数，表示 UTC 时间（通常是 `DATETIME` 类型）
   2. **timezone**：必需参数，表示目标时区（字符串，如 `'Asia/Shanghai'`）
6. unix_timestamp(string date, string format)
   1. **`date`**：需要转换的日期时间字符串。
   2. **`format`**：日期时间字符串的格式（可选，默认格式为 `'yyyy-MM-dd HH:mm:ss'`）。
   3. SELECT unix_timestamp();  返回当前时间戳







# 细节



## cast

hivesql中，对decimal类型的字段进行强制转换时，如原字段类型 col decimal(10,2)，结果需要转成 decimal(16,4) 的精度，如果使用  sum(cast(col as decimal(16, 4))) 进行先转换再sum会导致结果的小数点整体向左偏移两位，即结果比正常小了一百倍，而使用 cast(sum(col) as decimal(16,4)) 结果无异常。
**根本原因**：Hive 的 `CAST` 在调整 `DECIMAL` 小数位时，复用原始整数值并重新应用缩放比例，导致数值被错误缩放。

所以使用 cast(sum(col) as )的方式，先求和再转换，
在进行计算时，尽量避免不同精度类型的 `decimal` 之间进行隐式转换，这可能导致精度丢失。建议在计算前，将所有参与运算的 `decimal` 转换为相同精度，或者使用 `cast` 函数进行显式转换

decimal类型在sum计算时，Hive 的 `SUM` 函数会自动扩展精度（`+10`），为了防止聚合过程中的溢出风险。

对于decimal类型，在最终结果要使用 cast强转为目标表内的数据类型，中间数据计算时，低精度会隐式转换成高精度，低精度可以不做cast；

对于从高精度如decimal（16,6） cast 成 低精度 decimal(16,2)，hive会对要截断的第一位数据进行四舍五入

### **精度扩展的通用规则**

| 聚合函数  | 结果类型规则（输入 `DECIMAL(p,s)`）   |
| :-------- | :------------------------------------ |
| `SUM()`   | `DECIMAL(min(38, p+10), s)`           |
| `AVG()`   | `DECIMAL(min(38, p+4), min(s+4, 38))` |
| `MIN/MAX` | 保持原类型 `DECIMAL(p,s)`             |
| `COUNT`   | `BIGINT`（非 DECIMAL）                |





# 优化



## map倾斜

对于存储压缩格式是 gzip、snappy等不支持分片的数据格式，在只进行maptask 如单纯读取数据或执行mapjoin时，没有reduce阶段，会出现一个maptask读取了过多的任务，导致长尾的map倾斜，可以在读表的时候加上 `distribute by rand()` 将数据随机发送到不同的分区中，避免了map倾斜，可以搭配 `set mapred.max.split.size=32000000;`参数来控制maptask数量

eg

```sql
select *
from
    (
        select
            freight_code
          , order_code
          , store_id
          , transport_mode_id
          , track_number
          , status
          , create_time
          , delivery_time
          , cut_off_remark
          , cut_off_code
          , track_status
          , track_update_time
          , update_time_cn
          , delivery_time_cn
          , scan_time

        from
            dip_ods.ods_order_freight_ff
        where
            dt = '2000-01-01'
        union all
        select
            freight_code
          , order_code
          , store_id
          , transport_mode_id
          , track_number
          , status
          , create_time
          , delivery_time
          , cut_off_remark
          , cut_off_code
          , track_status
          , track_update_time
          , update_time_cn
          , delivery_time_cn
          , scan_time
        from
            dip_ods.ods_order_freight_his_ff
        where
            dt = '2000-01-01'
    ) a
    distribute by rand()
```


