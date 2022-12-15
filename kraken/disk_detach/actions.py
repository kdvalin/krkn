import yaml
import sys
import logging
import time

from ..node_actions.aws_node_scenarios import AWS
import kraken.cerberus.setup as cerberus

def run(scenarios_list, config, wait_duration):
    failed_post_scenarios = ""
    for disk_detach_scenario in scenarios_list:
        if len(disk_detach_scenario) > 1:
            with open(disk_detach_scenario, "r") as f:
                disk_detach_config = yaml.full_load(f)
                cloud_type = disk_detach_config["cloud_type"]
                instance_name = disk_detach_config["node_name"]
                device_path = disk_detach_config["device_path"]

                if cloud_type.lower() == "aws":
                    cloud_object = AWS()
                else:
                    logging.error(
                        "Cloud type %s is not currently supported for "
                        "disk detach scenarios"
                        % cloud_type
                    )
                    sys.exit(1)
                start_time = int(time.time())

                instance_id = cloud_object.get_instance_id(instance_name)
                disk_id = cloud_object.get_instance_disk(instance_id, device_path)

                logging.info("Detaching Disk with config \n %s" % disk_detach_config)
                try:
                    cloud_object.detach_disk(instance_id, disk_id)
                except Exception as e:
                    logging.error("Could not detach disk %s from %s \n %s" % (disk_id, instance_id, e))
                    sys.exit(1)
                
                logging.info("Waiting for wait_duration %s" % wait_duration)
                time.sleep(wait_duration)

                logging.info("Reattaching disk with config \n %s" % disk_detach_config)

                try:
                    cloud_object.attach_disk(instance_id, disk_id, device_path)
                except Exception as e:
                    logging.error("Could not attach disk %s to instance %s \n %s" % (disk_id, instance_id, e))
                    sys.exit(1)

                end_time = int(time.time())
                cerberus.publish_kraken_status(config, failed_post_scenarios, start_time, end_time)
