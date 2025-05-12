import taos
import pandas as pd
from config.config import TaosConfig

taos_config = TaosConfig.parse_from_config()
conn = taos.connect(host=taos_config.host, user=taos_config.user, password=taos_config.password)

cursor = conn.cursor()

# 执行查询
cursor.execute(' SELECT * FROM   md4x_7s.tb_654207003 limit 20')

# 获取查询结果
data = cursor.fetchall()
df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
print(df)

cursor.close()
conn.close()
