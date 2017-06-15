from Apollo.Configurations import max_retry_count, time_sleep_pubish_error

def retry(handle_task, handle_exception, handle_exceed_retry, arg):
    retry_number = max_retry_count
    delay_interval = time_sleep_pubish_error
    for i in range(0, retry_number):
        try:
            handle_task(arg)
            return
        except Exception as exp:
            handle_exception(exp)
            time.sleep(delay_interval)
    handle_exceed_retry()