from datetime import datetime, date
from typing import List, Dict, Any, Tuple
from fastapi import FastAPI, HTTPException, Query
import taos
from config.config import TaosConfig
from pydantic import BaseModel
from contextlib import contextmanager


taos_config = TaosConfig.parse_from_config()

app = FastAPI()
class CustomResponse(BaseModel):
    """自定义响应模型"""
    code: int  # 状态码
    data: Dict[str, Any]  # 响应数据
    page_num: int
    page_size: int
    total: int  # 新增总记录数字段
    message: str  # 附加消息


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
    return data[0][0] if data else 0.0

def sampling_intervall(cursor, table_name: str, start: str, end: str) -> float:
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
    return data[0][0] if data else 0.0

def data_proportions(cursor, table_name: str, start: str, end: str) -> float:
    """有效数据和限功率数据"""
    query_sql = f"""
    SELECT 
           avg(wtur_bool_rd_b0_normalfeedback) as avg_wtur_state_rn_i8,
           avg(wcnv_bool_rd_b0_warmupin) as avg_wtur_other_rn_i16_limpow
    FROM {table_name}
    WHERE ts > '{start} 00:00:00.000' and ts <= '{end} 23:59:59.000'
    GROUP BY  wtid;
    """
    cursor.execute(query_sql)
    data = cursor.fetchall()
    return data[0][0] if data else 0.0

def power_gens(cursor, table_name: str, start: str, end: str) -> float:
    """有效数据和限功率数据"""
    query_sql = f"""
   SELECT 
         sum(power_gen) AS power
    FROM
        (SELECT 
             (DIFF(ts) / 1000 / 3600) * wtur_other_ra_f32_cpu AS power_gen
        FROM {table_name}
        WHERE ts > '{start} 00:00:00.000' and ts <= '{end} 23:59:59.000')
    
    """
    cursor.execute(query_sql)
    data = cursor.fetchall()

    return data[0][0] if data else 0.0



@app.get("/md4x3.0/wfquality/wfquality/7s/power_gen", response_model=CustomResponse)
async def get_wfquality_completeness(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """获取风场实际发电量(带分页功能)"""
    try:
        # 验证日期格式
        datetime.strptime(start, '%Y-%m-%d')
        datetime.strptime(end, '%Y-%m-%d')

        # 获取该风场下的所有风机ID
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data={"wtid": [], "value": []},
                page_num=page_num,
                page_size=page_size,
                total=0,
                message="该风场下未找到任何风机"
            )

        # 计算所有风机的完整性指标
        values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    completeness = power_gens(
                        cursor, table_name, start, end
                    )
                    values.append(completeness)
                except Exception:
                    values.append(0.0)
                    continue

        # 应用分页
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_values, _ = paginate_data(values, page_num, page_size)

        return CustomResponse(
            code=0,
            data={
                "wtid": paginated_wtid,
                "value": paginated_values
            },
            page_num=page_num,
            page_size=page_size,
            total=total,
            message=""
        )

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="日期格式无效，请使用YYYY-MM-DD格式"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )









@app.get("/md4x3.0/wfquality/wfquality/7s/data_proportions", response_model=CustomResponse)
async def get_wfquality_completeness(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """获取风场数据完整性指标(带分页功能)"""
    try:
        # 验证日期格式
        datetime.strptime(start, '%Y-%m-%d')
        datetime.strptime(end, '%Y-%m-%d')

        # 获取该风场下的所有风机ID
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data={"wtid": [], "value": []},
                page_num=page_num,
                page_size=page_size,
                total=0,
                message="该风场下未找到任何风机"
            )

        # 计算所有风机的完整性指标
        values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    completeness = data_proportions(
                        cursor, table_name, start, end
                    )
                    values.append(completeness)
                except Exception:
                    values.append(0.0)
                    continue


        # 应用分页
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_values, _ = paginate_data(values, page_num, page_size)

        return CustomResponse(
            code=0,
            data={
                "wtid": paginated_wtid,
                "value": paginated_values
            },
            page_num=page_num,
            page_size=page_size,
            total=total,
            message=""
        )

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="日期格式无效，请使用YYYY-MM-DD格式"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )



@app.get("/md4x3.0/wfquality/wfquality/7s/completeness", response_model=CustomResponse)
async def get_wfquality_completeness(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """获取风场数据完整性指标(带分页功能)"""
    try:
        # 验证日期格式
        datetime.strptime(start, '%Y-%m-%d')
        datetime.strptime(end, '%Y-%m-%d')

        # 获取该风场下的所有风机ID
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data={"wtid": [], "value": []},
                page_num=page_num,
                page_size=page_size,
                total=0,
                message="该风场下未找到任何风机"
            )

        # 计算所有风机的完整性指标
        values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    completeness = calculate_completeness(
                        cursor, table_name, start, end
                    )
                    values.append(completeness)
                except Exception:
                    values.append(0.0)
                    continue


        # 应用分页
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_values, _ = paginate_data(values, page_num, page_size)

        return CustomResponse(
            code=0,
            data={
                "wtid": paginated_wtid,
                "value": paginated_values
            },
            page_num=page_num,
            page_size=page_size,
            total=total,
            message=""
        )

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="日期格式无效，请使用YYYY-MM-DD格式"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )


@app.get("/md4x3.0/wfquality/wfquality/7s/sampling_interval", response_model=CustomResponse)
async def get_wfquality_completeness(
        wf_id: int = Query(default=654207, description="风场ID"),
        start: str = Query(default='2024-12-21', description="开始日期(YYYY-MM-DD)"),
        end: str = Query(default='2024-12-23', description="结束日期(YYYY-MM-DD)"),
        page_num: int = Query(default=1, gt=0, description="页码，从1开始"),
        page_size: int = Query(default=10, gt=0, le=100, description="每页数量，最大100")
):
    """获取风场数据完整性指标(带分页功能)"""
    try:
        # 验证日期格式
        datetime.strptime(start, '%Y-%m-%d')
        datetime.strptime(end, '%Y-%m-%d')

        # 获取该风场下的所有风机ID
        wtid_list = get_wtid_list(wf_id)
        if not wtid_list:
            return CustomResponse(
                code=0,
                data={"wtid": [], "value": [], "average": 0},
                page_num=page_num,
                page_size=page_size,
                total=0,
                message="该风场下未找到任何风机"
            )

        # 计算所有风机的完整性指标
        values = []
        with tdengine_connection() as conn:
            cursor = conn.cursor()
            for wtid in wtid_list:
                table_name = concat_child_table_name(wtid)
                try:
                    completeness = sampling_intervall(
                        cursor, table_name, start, end
                    )
                    values.append(completeness)
                except Exception:
                    values.append(0.0)
                    continue


        # 应用分页
        paginated_wtid, total = paginate_data(wtid_list, page_num, page_size)
        paginated_values, _ = paginate_data(values, page_num, page_size)

        return CustomResponse(
            code=0,
            data={
                "wtid": paginated_wtid,
                "value": paginated_values
            },
            page_num=page_num,  
            page_size=page_size,
            total=total,
            message=""
        )

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="日期格式无效，请使用YYYY-MM-DD格式"
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
