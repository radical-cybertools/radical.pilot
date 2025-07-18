from datetime import datetime
from enum import Enum
import sys

class Colors:
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class Logger:
    def __init__(self, name="DDMDManager", use_colors=True, output_stream=None):
        self.name = name
        self.use_colors = use_colors
        self.output_stream = output_stream or sys.stdout
        
        self.level_colors = {
            LogLevel.DEBUG: Colors.BRIGHT_BLACK,
            LogLevel.INFO: Colors.BRIGHT_CYAN,
            LogLevel.WARNING: Colors.BRIGHT_YELLOW,
            LogLevel.ERROR: Colors.BRIGHT_RED,
            LogLevel.CRITICAL: Colors.RED + Colors.BOLD
        }
        
        self.component_colors = {
            'task': Colors.BRIGHT_GREEN,
            #'adaptive': Colors.BRIGHT_MAGENTA,
            'manager': Colors.BRIGHT_BLUE,
            'workflow': Colors.CYAN,
            'task': Colors.YELLOW,
            'error': Colors.RED,
            'success': Colors.GREEN,
            'stage': Colors.BRIGHT_CYAN,
            'step': Colors.CYAN,
            'resource': Colors.MAGENTA,
            'data': Colors.BRIGHT_YELLOW,
            'validation': Colors.BRIGHT_MAGENTA,
            'checkpoint': Colors.BRIGHT_GREEN,
            'metric': Colors.BRIGHT_WHITE
        }

    def _colorize(self, text, color):
        return f"{color}{text}{Colors.RESET}" if self.use_colors else text

    def _format_message(self, level, component, message, task_name=None):
        timestamp = self._colorize(datetime.now().strftime("%H:%M:%S.%f")[:-3], Colors.DIM)
        colored_level = self._colorize(f"[{level.value}]", self.level_colors.get(level, Colors.WHITE))
        
        # Handle task-specific components
        if component.lower().startswith('task-'):
            component_color = Colors.BRIGHT_GREEN
        else:
            component_color = self.component_colors.get(component.lower(), Colors.WHITE)
        
        colored_component = self._colorize(f"[{component.upper()}]", component_color)
        
        task_part = ""
        if task_name:
            task_part = f" {self._colorize(f'[{task_name}]', Colors.BRIGHT_WHITE)}"
        
        return f"{timestamp} {colored_level} {colored_component}{task_part} {message}"

    def _write_log(self, message, to_stderr=False):
        stream = sys.stderr if to_stderr else self.output_stream
        stream.write(message + '\n')
        stream.flush()

    def debug(self, message, component="manager", task_name=None):
        formatted = self._format_message(LogLevel.DEBUG, component, message, task_name)
        self._write_log(formatted)

    def info(self, message, component="manager", task_name=None):
        formatted = self._format_message(LogLevel.INFO, component, message, task_name)
        self._write_log(formatted)

    def warning(self, message, component="manager", task_name=None):
        formatted = self._format_message(LogLevel.WARNING, component, message, task_name)
        self._write_log(formatted)

    def error(self, message, component="manager", task_name=None):
        formatted = self._format_message(LogLevel.ERROR, component, message, task_name)
        self._write_log(formatted, to_stderr=True)

    def critical(self, message, component="manager", task_name=None):
        formatted = self._format_message(LogLevel.CRITICAL, component, message, task_name)
        self._write_log(formatted, to_stderr=True)

    def task_started(self, task_name):
        message = f"Task started: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
        self.info(message, "manager")

    def task_completed(self, task_name):
        message = f"Task completed: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
        self.info(message, "manager")

    def task_killed(self, task_name):
        message = f"Task killed: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
        self.warning(message, "task")

    # def adaptive_started(self, task_name):
    #     message = f"Adaptive function started for: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
    #     self.info(message, "adaptive")

    # def adaptive_completed(self, task_name):
    #     message = f"Adaptive function completed for: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
    #     self.info(message, "adaptive")

    # def adaptive_failed(self, task_name, error):
    #     message = f"Adaptive function failed for {self._colorize(task_name, Colors.BRIGHT_WHITE)}: {error}"
    #     self.error(message, "adaptive")

    # def child_task_submitted(self, child_name, parent_name):
    #     message = f"Submitting child task: {self._colorize(child_name, Colors.BRIGHT_WHITE)} from {self._colorize(parent_name, Colors.BRIGHT_WHITE)}"
    #     self.info(message, "manager")

    def manager_starting(self, task_count):
        message = f"Starting with {self._colorize(str(task_count), Colors.BRIGHT_WHITE)} initial tasks"
        self.info(message, "manager")

    def manager_exiting(self):
        self.info("All tasks finished. Exiting.", "manager")

    # def activity_summary(self, active_tasks, active_adaptive, buffered_tasks):
    #     summary = (f"Active: {self._colorize(str(active_tasks), Colors.BRIGHT_GREEN)} tasks, "
    #               f"{self._colorize(str(active_adaptive), Colors.BRIGHT_MAGENTA)} adaptive tasks, "
    #               f"{self._colorize(str(buffered_tasks), Colors.BRIGHT_YELLOW)} buffered")
    #     self.debug(summary, "manager")

    def task_log(self, message, level=LogLevel.INFO):
        task_component = f"TASK-{self.name.upper()}"
        formatted = self._format_message(level, task_component, message)
        self._write_log(formatted, to_stderr=level in [LogLevel.ERROR, LogLevel.CRITICAL])

    def separator(self, title=None):
        if title:
            separator = f"{'='*20} {title} {'='*20}"
        else:
            separator = "="*50
        self._write_log(self._colorize(separator, Colors.BRIGHT_BLUE))