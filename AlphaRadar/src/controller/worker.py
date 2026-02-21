from PyQt6.QtCore import QRunnable, pyqtSlot, QObject, pyqtSignal
import traceback
import sys
from typing import Tuple, Any, Callable

class WorkerSignals(QObject):
    """
    Worker 线程信号定义 (Worker Signals).
    
    Attributes:
        finished (pyqtSignal): 任务完成信号 (无参数).
        error (pyqtSignal): 错误信号 (tuple: exctype, value, traceback).
        result (pyqtSignal): 结果信号 (object: 任务返回值).
    """
    finished = pyqtSignal()
    error = pyqtSignal(tuple)
    result = pyqtSignal(object)

class Worker(QRunnable):
    """
    异步工作线程 (Async Worker Thread).
    继承自 QRunnable，用于在后台线程池中执行任务，避免阻塞 UI.
    
    Args:
        fn (Callable): 需要执行的回调函数.
        *args: 回调函数的位置参数.
        **kwargs: 回调函数的关键字参数.
    """

    def __init__(self, fn: Callable, *args: Any, **kwargs: Any) -> None:
        super(Worker, self).__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self) -> None:
        """
        执行 Worker 逻辑.
        包含完整的异常捕获，通过信号传递结果或错误信息.
        """
        try:
            result = self.fn(*self.args, **self.kwargs)
        except:
            traceback.print_exc()
            exctype, value = sys.exc_info()[:2]
            # 传递错误信息
            self.signals.error.emit((exctype, value, traceback.format_exc()))
        else:
            # 传递执行结果
            self.signals.result.emit(result)
        finally:
            # 发送完成信号
            self.signals.finished.emit()
