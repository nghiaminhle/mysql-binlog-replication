from .performance_metric import PerformanceMetric
import sys
import logging

class PerformanceReporter:
    
    def report(self, metric:PerformanceMetric):
        print_format = self._get_print_format()
        msg = print_format.format(
                metric.reported_at,
                metric.pending_queue_count, 
                metric.read_log_per_seconds,
                metric.filter_log_per_seconds,
                metric.publish_per_seconds,
                metric.commit_count_per_seconds,
                metric.commit_queue_count,
                metric.read_log_count,
                metric.filter_log_count,
                metric.publish_count,
                metric.commit_count,
                metric.holing_checkpoint_counts,
                metric.waiting_checkpoint_counts
                )
        print(msg)
        #logging.basicConfig(filename='monitor.log',level=logging.DEBUG)
        #logging.info(msg)
    
    def _get_print_format(self):
        print_format = """
            ------------Monitor------------
            Reported At {}
            -------------------------------
            1. Pending Queue:{}
            2. Read Log Per Seconds: {}
            3. Filter Log Per Seconds: {}
            4. Publish To Kafka Per Seconds: {}
            5. Total Commit Per Seconds: {}
            6. Commit Queue:{}
            7. Total Read Log: {}
            8. Total Filter Log: {}
            9. Total Publish To Kafka: {}
            10. Total Commit: {}
            11. Holding Checkpoint Counts: {}
            12. Waiting Checkpoint Counts: {}
            -------------------------------
             """

        return print_format