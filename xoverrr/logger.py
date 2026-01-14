import logging

app_logger = logging.getLogger(__name__)
app_logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - xoverrr.%(module)s.%(funcName)s - %(message)s')

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
app_logger.addHandler(console_handler)