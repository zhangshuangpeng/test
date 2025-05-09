from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, HTTPException, Query
import taos
from md4x.config.config import TaosConfig
from pydantic import BaseModel
from contextlib import contextmanager

app = FastAPI()
taos_config = TaosConfig.parse_from_config()

# ========== 数据模型定义 ==========
class ValueData(BaseModel):
    """数据值模型"""
    valid: List[float]
    limited: List[float]

class MetricData(BaseModel):
    """指标数据模型"""
    values: List[float]
    average: float

class ResponseData(BaseModel):
    """统一响应数据模型"""
    total: int
    page_num: int
    page_size: int
    wtid: List[str]
    value: Optional[Any] = None  # 可以是ValueData或MetricData

class CustomResponse(BaseModel):
    """统一API响应模型"""
    code: int
    data: ResponseData
    message: str


def concat_super_table_name(wf_id: int) -> str:
    """根据风场ID生成超级表名称"""
    return f"stb_{wf_id}"

def concat_child_table_name(wtid: str) -> str:
    """根据风机ID生成子表名称"""
    return f"tb_{wtid}"

@contextmanager
def tdengine_connection():
    """TDengine连接上下文管理器"""
    conn = None
    try:
        conn = taos.connect(
            host=taos_config.host,
            user=taos_config.user,
            password=taos_config.password,
            database=taos_config.database
        )
        yield conn
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"TDengine连接失败: {str(e)}"
        )
    finally:
        if conn:
            conn.close()

def get_wtid_list(wfid: int) -> List[str]:
    """获取指定风场下的所有风机ID列表"""
    with tdengine_connection() as conn:
        cursor = conn.cursor()
        try:
            super_table_name = concat_super_table_name(wfid)
            sql = f"""
                SELECT SUBSTR(tbname, 4) as device 
                FROM {super_table_name} 
                GROUP BY tbname
                ORDER BY device
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            return [str(row[0]) for row in results] if results else []
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"查询风机列表失败: {str(e)}"
            )

def paginate_data(data: List[Any], page_num: int, page_size: int) -> Tuple[List[Any], int]:
    """通用分页函数"""
    total = len(data)
    start = (page_num - 1) * page_size
    end = start + page_size
    paginated_data = data[start:end]
    return paginated_data, total

def validate_dates(start: str, end: str):
    """验证日期格式和逻辑"""
    try:
        start_date = datetime.strptime(start, '%Y-%m-%d').date()
        end_date = datetime.strptime(end, '%Y-%m-%d').date()
        if start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")
        return start_date, end_date
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e) if str(e) else "日期格式无效，请使用YYYY-MM-DD格式"
        )

def calculate_completeness(cursor, table_name: str, start: str, end: str) -> float:
    """计算风机的数据完整性指标"""
    query_sql = f"""
        SELECT diff_total / TIMEDIFF('{start}', '{end}', 1h) AS completeness
        FROM 
            (SELECT sum(diff) / 3600 AS diff_total
            FROM 
                (SELECT DIFF(ts) / 1000 AS diff
                FROM {table_name}
                WHERE ts > '{start}'
                    AND ts <= '{end}') )
    """
    cursor.execute(query_sql)
    data = cursor.fetchall()
    return float(data[0][0]) if data else 0.0

def sampling_interval(cursor, table_name: str, start: str, end: str) -> float:
    """计算风机的采样频率"""
    query_sql = f"""
    SELECT avg(ts_diff) AS diff_mean
    FROM
        (
    SELECT
             DIFF(ts) / 1000 AS ts_diff
        FROM {table_name}
        WHERE ts > '{start} 00:00:00.000' and ts <= '{end} 23:59:59.000'
    )
    """
    cursor.execute(query_sql)
    data = cursor.fetchall()
    return float(data[0][0]) if data else 0.0


def data_proportions(cursor, table_name: str, start: str, end: str) -> Tuple[float, float]:
    query_sql = f"""
        SELECT
            avg(CASE WHEN wcnv_bool_rd_b0_warmupin THEN 1 ELSE 0 END),
            avg(CASE WHEN wcnv_bool_rd_b0_warmupin THEN 1 ELSE 0 END)
        FROM {table_name}
        WHERE ts > '{start} 00:00:00.000' AND ts <= '{end} 23:59:59.000'
    """
    cursor.execute(query_sql)
    data = cursor.fetchall()

    if data:  # 检查是否有数据
        row = data[0]
        return (float(row[0]), float(row[1]))
    return (0.0, 0.0)

@app.get("/md4x3.0/wfquality/wfquality/7s/data_proportions", response_model=CustomResponse)
async def get_data_proportions(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """
    获取风场数据质量指标(有效值和限值比例)
    - wf_id: 风场ID
    - start: 开始日期(YYYY-MM-DD)
    - end: 结束日期(YYYY-MM-DD)
    - page_num: 页码
    - page_size: 每页数量
    """
    try:
        # 验证日期
        validate_dates(start, end)

        # 获取风机列表
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data=ResponseData(
                    total=0,
                    page_num=page_num,
                    page_size=page_size,
                    wtid=[],
                    value=ValueData(valid=[], limited=[])
                ),
                message="该风场下未找到任何风机"
            )

        # 计算指标
        valid_values = []
        limited_values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    valid, limited = data_proportions(cursor, table_name, start, end)
                    valid_values.append(valid)
                    limited_values.append(limited)
                except Exception as e:
                    print(f"处理风机{wtid}时出错: {str(e)}")
                    valid_values.append(0.0)
                    limited_values.append(0.0)

        # 分页处理
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_valid, _ = paginate_data(valid_values, page_num, page_size)
        paginated_limited, _ = paginate_data(limited_values, page_num, page_size)

        return CustomResponse(
            code=0,
            data=ResponseData(
                total=total,
                page_num=page_num,
                page_size=page_size,
                wtid=paginated_wtid,
                value=ValueData(
                    valid=paginated_valid,
                    limited=paginated_limited
                )
            ),
            message="请求成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )

@app.get("/md4x3.0/wfquality/wfquality/7s/completeness", response_model=CustomResponse)
async def get_completeness_metrics(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """
    获取风场数据完整性指标
    - wf_id: 风场ID
    - start: 开始日期(YYYY-MM-DD)
    - end: 结束日期(YYYY-MM-DD)
    - page_num: 页码
    - page_size: 每页数量
    """
    try:
        # 验证日期
        validate_dates(start, end)

        # 获取风机列表
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data=ResponseData(
                    total=0,
                    page_num=page_num,
                    page_size=page_size,
                    wtid=[],
                    value=MetricData(values=[], average=0)
                ),
                message="该风场下未找到任何风机"
            )

        # 计算指标
        values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    completeness = calculate_completeness(cursor, table_name, start, end)
                    values.append(completeness)
                except Exception as e:
                    print(f"处理风机{wtid}时出错: {str(e)}")
                    values.append(0.0)

        # 计算平均值
        average = sum(values) / len(values) if values else 0

        # 分页处理
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_values, _ = paginate_data(values, page_num, page_size)

        return CustomResponse(
            code=0,
            data=ResponseData(
                total=total,
                page_num=page_num,
                page_size=page_size,
                wtid=paginated_wtid,
                value=MetricData(
                    values=paginated_values,
                    average=average
                )
            ),
            message="请求成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )

@app.get("/md4x3.0/wfquality/wfquality/7s/sampling_interval", response_model=CustomResponse)
async def get_sampling_interval(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """
    获取风场数据采样间隔指标
    - wf_id: 风场ID
    - start: 开始日期(YYYY-MM-DD)
    - end: 结束日期(YYYY-MM-DD)
    - page_num: 页码
    - page_size: 每页数量
    """
    try:
        # 验证日期
        validate_dates(start, end)

        # 获取风机列表
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data=ResponseData(
                    total=0,
                    page_num=page_num,
                    page_size=page_size,
                    wtid=[],
                    value=MetricData(values=[], average=0)
                ),
                message="该风场下未找到任何风机"
            )

        # 计算指标
        values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    interval = sampling_interval(cursor, table_name, start, end)
                    values.append(interval)
                except Exception as e:
                    print(f"处理风机{wtid}时出错: {str(e)}")
                    values.append(0.0)

        # 计算平均值
        average = sum(values) / len(values) if values else 0

        # 分页处理
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_values, _ = paginate_data(values, page_num, page_size)

        return CustomResponse(
            code=0,
            data=ResponseData(
                total=total,
                page_num=page_num,
                page_size=page_size,
                wtid=paginated_wtid,
                value=MetricData(
                    values=paginated_values,
                    average=average
                )
            ),
            message="请求成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)