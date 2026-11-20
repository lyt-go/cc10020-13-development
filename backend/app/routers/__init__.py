"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline

# pipeline 固定前缀 /api/pipeline 必须先注册，否则其 GET 端点会被
# 资源路由的 /api/{module}/{entry_id} 动态段抢先匹配（entry_id 解析为 int 报 422）。
ROUTERS = [pipeline, resource]
