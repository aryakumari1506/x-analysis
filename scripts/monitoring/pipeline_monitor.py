import logging
import time
import psutil
import os
from datetime import datetime
import json

class PipelineMonitor:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('monitoring.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def check_system_resources(self):
        """Monitor system resources"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_available_gb': memory.available / (1024**3),
            'disk_percent': disk.percent,
            'disk_free_gb': disk.free / (1024**3)
        }
        
        self.logger.info(f"System metrics: {json.dumps(metrics, indent=2)}")
        return metrics
        
    def check_hadoop_services(self):
        """Check if Hadoop services are running"""
        try:
            # Check if namenode is accessible
            import requests
            response = requests.get('http://localhost:9870', timeout=5)
            namenode_status = response.status_code == 200
        except:
            namenode_status = False
            
        self.logger.info(f"Hadoop NameNode status: {'Running' if namenode_status else 'Down'}")
        return namenode_status
        
    def check_spark_services(self):
        """Check if Spark services are running"""
        try:
            import requests
            response = requests.get('http://localhost:8080', timeout=5)
            spark_status = response.status_code == 200
        except:
            spark_status = False
            
        self.logger.info(f"Spark Master status: {'Running' if spark_status else 'Down'}")
        return spark_status
        
    def check_data_freshness(self):
        """Check if data is being updated regularly"""
        try:
            data_dir = 'data/raw'
            if os.path.exists(data_dir):
                files = os.listdir(data_dir)
                if files:
                    latest_file = max([os.path.join(data_dir, f) for f in files], 
                                    key=os.path.getctime)
                    file_age = time.time() - os.path.getctime(latest_file)
                    
                    # Alert if data is older than 1 hour
                    if file_age > 3600:
                        self.logger.warning(f"Data is stale! Latest file is {file_age/3600:.1f} hours old")
                        return False
                    else:
                        self.logger.info(f"Data is fresh. Latest file is {file_age/60:.1f} minutes old")
                        return True
                else:
                    self.logger.warning("No data files found")
                    return False
            else:
                self.logger.error("Data directory does not exist")
                return False
        except Exception as e:
            self.logger.error(f"Error checking data freshness: {e}")
            return False
            
    def generate_health_report(self):
        """Generate comprehensive health report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'system_resources': self.check_system_resources(),
            'hadoop_status': self.check_hadoop_services(),
            'spark_status': self.check_spark_services(),
            'data_freshness': self.check_data_freshness()
        }
        
        # Save report
        os.makedirs('reports', exist_ok=True)
        report_file = f"reports/health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        self.logger.info(f"Health report saved to {report_file}")
        return report

if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.generate_health_report()