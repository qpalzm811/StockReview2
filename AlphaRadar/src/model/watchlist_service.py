import logging
from PyQt6.QtCore import QObject, pyqtSignal
from .db_manager import DBManager

class WatchlistService(QObject):
    """
    自选股服务 (Watchlist Service).
    负责管理自选股的分组、添加、删除以及通知 UI 更新。
    """
    # 信号
    watchlist_updated = pyqtSignal() # 当自选股列表发生变化时发送
    groups_updated = pyqtSignal()    # 当分组发生变化时发送

    def __init__(self, db_manager: DBManager):
        super().__init__()
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def get_groups(self) -> list:
        """获取所有分组名称."""
        return self.db.get_watchlist_groups()

    def get_stocks(self, group_name: str):
        """获取指定分组的股票列表 (DataFrame)."""
        return self.db.get_watchlist(group_name)

    def add_stock(self, symbol: str, group_name: str = "Default"):
        """添加股票到自选 (自动处理去重)."""
        try:
            self.db.add_watchlist_item(symbol, group_name)
            self.logger.info(f"Added {symbol} to watchlist group '{group_name}'")
            self.watchlist_updated.emit()
            # 如果是新分组，可能会通过 get_groups 检查出来，但这里我们假设 UI 会刷新
            # 如果 group_name 是新的，或许应该 emit groups_updated
            # 简单起见，这里 emit 两个信号
            self.groups_updated.emit()
        except Exception as e:
            self.logger.error(f"Failed to add stock {symbol}: {e}")

    def remove_stock(self, symbol: str, group_name: str):
        """从自选中移除股票."""
        try:
            self.db.remove_watchlist_item(symbol, group_name)
            self.logger.info(f"Removed {symbol} from watchlist group '{group_name}'")
            self.watchlist_updated.emit()
        except Exception as e:
            self.logger.error(f"Failed to remove stock {symbol}: {e}")

    def create_group(self, group_name: str):
        """创建一个空分组 (通过添加一个假占位符? 或者数据库设计允许空?).
        目前的 DB 设计是 PKKey(symbol, group_name)。
        如果 symbol 必须存在，我们无法创建空分组。
        Futu 允许空分组。
        
        [Workaround] 
        目前简单实现：分组随股票存在。
        如果用户想要新建分组，必须拖入第一个股票时指定新名称。
        或者我们可以插入一个特殊 symbol='_META_' 来占位。
        
        暂不支持纯空分组持久化，必须随股票创建。
        """
        pass
