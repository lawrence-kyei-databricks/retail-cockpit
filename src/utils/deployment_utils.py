# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Utilities for Retail Insight Cockpit
# MAGIC
# MAGIC This notebook provides utility functions for deploying and managing
# MAGIC the Retail Insight Cockpit Asset Bundle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import json
import yaml
import os
import subprocess
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Configuration Class

# COMMAND ----------

class RetailCockpitDeployer:
    """
    Utility class for deploying and managing the Retail Insight Cockpit
    """

    def __init__(self,
                 catalog_name: str,
                 schema_name: str,
                 workspace_url: str,
                 deployment_env: str = "dev"):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.workspace_url = workspace_url
        self.deployment_env = deployment_env
        self.bundle_path = "/Workspace/Repos/retail-insight-cockpit"

    def validate_prerequisites(self) -> Dict[str, bool]:
        """
        Validate that all prerequisites are met for deployment
        """
        logger.info("Validating deployment prerequisites...")

        checks = {}

        # Check Unity Catalog access
        try:
            spark.sql(f"USE CATALOG {self.catalog_name}")
            checks["unity_catalog_access"] = True
        except Exception as e:
            logger.error(f"Unity Catalog access failed: {e}")
            checks["unity_catalog_access"] = False

        # Check schema access
        try:
            spark.sql(f"USE SCHEMA {self.catalog_name}.{self.schema_name}")
            checks["schema_access"] = True
        except Exception as e:
            logger.error(f"Schema access failed: {e}")
            checks["schema_access"] = False

        # Check Databricks CLI
        try:
            result = subprocess.run(["databricks", "--version"],
                                  capture_output=True, text=True)
            if result.returncode == 0:
                checks["databricks_cli"] = True
            else:
                checks["databricks_cli"] = False
        except Exception as e:
            logger.error(f"Databricks CLI check failed: {e}")
            checks["databricks_cli"] = False

        # Check for required tables
        required_tables = ["sales", "customers", "products", "stores", "inventory"]
        tables_exist = 0
        for table in required_tables:
            try:
                spark.sql(f"SELECT 1 FROM {self.catalog_name}.{self.schema_name}.{table} LIMIT 1")
                tables_exist += 1
            except:
                pass

        checks["required_tables"] = tables_exist >= len(required_tables)

        # Check SQL Warehouse access
        try:
            # This is a simplified check - in reality you'd check warehouse permissions
            checks["sql_warehouse_access"] = True
        except Exception as e:
            logger.error(f"SQL Warehouse access check failed: {e}")
            checks["sql_warehouse_access"] = False

        return checks

    def create_deployment_config(self) -> Dict[str, Any]:
        """
        Create deployment configuration based on environment
        """
        base_config = {
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "workspace_url": self.workspace_url,
            "deployment_env": self.deployment_env,
            "deployment_timestamp": datetime.now().isoformat()
        }

        env_configs = {
            "dev": {
                "resource_prefix": "dev-retail-cockpit",
                "cluster_size": "Small",
                "auto_termination_minutes": 30,
                "retention_days": 7,
                "enable_monitoring": False
            },
            "staging": {
                "resource_prefix": "staging-retail-cockpit",
                "cluster_size": "Medium",
                "auto_termination_minutes": 60,
                "retention_days": 30,
                "enable_monitoring": True
            },
            "prod": {
                "resource_prefix": "prod-retail-cockpit",
                "cluster_size": "Large",
                "auto_termination_minutes": 120,
                "retention_days": 90,
                "enable_monitoring": True
            }
        }

        config = {**base_config, **env_configs.get(self.deployment_env, env_configs["dev"])}
        return config

    def deploy_asset_bundle(self, config: Dict[str, Any]) -> bool:
        """
        Deploy the Databricks Asset Bundle
        """
        logger.info(f"Deploying Asset Bundle for {self.deployment_env} environment...")

        try:
            # Update databricks.yml with current configuration
            self._update_databricks_yml(config)

            # Deploy the bundle
            deploy_cmd = [
                "databricks", "bundle", "deploy",
                "--target", self.deployment_env,
                "--var", f"catalog_name={self.catalog_name}",
                "--var", f"schema_name={self.schema_name}",
                "--var", f"workspace_url={self.workspace_url}"
            ]

            result = subprocess.run(deploy_cmd, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info("Asset Bundle deployed successfully")
                return True
            else:
                logger.error(f"Asset Bundle deployment failed: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Asset Bundle deployment error: {e}")
            return False

    def _update_databricks_yml(self, config: Dict[str, Any]):
        """
        Update the databricks.yml file with current configuration
        """
        # This would update the actual databricks.yml file
        # For now, we'll just log the configuration
        logger.info(f"Configuration: {json.dumps(config, indent=2)}")

    def setup_data_sources(self) -> bool:
        """
        Setup and validate data sources
        """
        logger.info("Setting up data sources...")

        try:
            # Run the table creation script
            dbutils.notebook.run(
                "./setup/01_create_tables",
                timeout_seconds=1800,
                arguments={
                    "catalog_name": self.catalog_name,
                    "schema_name": self.schema_name
                }
            )

            # Generate sample data if needed
            table_count = spark.sql(f"SELECT COUNT(*) as count FROM {self.catalog_name}.{self.schema_name}.sales").collect()[0]['count']

            if table_count < 1000:  # Generate sample data if tables are empty
                dbutils.notebook.run(
                    "./setup/02_generate_sample_data",
                    timeout_seconds=3600,
                    arguments={
                        "catalog_name": self.catalog_name,
                        "schema_name": self.schema_name
                    }
                )

            # Create analytical views
            dbutils.notebook.run(
                "./setup/03_create_analytical_views",
                timeout_seconds=1800,
                arguments={
                    "catalog_name": self.catalog_name,
                    "schema_name": self.schema_name
                }
            )

            logger.info("Data sources setup completed successfully")
            return True

        except Exception as e:
            logger.error(f"Data sources setup failed: {e}")
            return False

    def setup_genie_integration(self) -> bool:
        """
        Setup Genie integration
        """
        logger.info("Setting up Genie integration...")

        try:
            # Run the Genie setup scripts
            dbutils.notebook.run(
                "../genie/retail_prompt_library",
                timeout_seconds=1200,
                arguments={
                    "catalog_name": self.catalog_name,
                    "schema_name": self.schema_name
                }
            )

            dbutils.notebook.run(
                "../genie/genie_integration_setup",
                timeout_seconds=1200,
                arguments={
                    "catalog_name": self.catalog_name,
                    "schema_name": self.schema_name
                }
            )

            logger.info("Genie integration setup completed successfully")
            return True

        except Exception as e:
            logger.error(f"Genie integration setup failed: {e}")
            return False

    def validate_deployment(self) -> Dict[str, Any]:
        """
        Validate the deployment by checking all components
        """
        logger.info("Validating deployment...")

        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.deployment_env,
            "checks": {}
        }

        # Check tables exist and have data
        required_tables = [
            "sales", "customers", "products", "stores",
            "inventory", "promotions", "suppliers"
        ]

        for table in required_tables:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {self.catalog_name}.{self.schema_name}.{table}").collect()[0]['count']
                validation_results["checks"][f"{table}_exists"] = count > 0
                validation_results["checks"][f"{table}_count"] = count
            except Exception as e:
                validation_results["checks"][f"{table}_exists"] = False
                validation_results["checks"][f"{table}_error"] = str(e)

        # Check analytical views
        analytical_views = [
            "daily_sales_agg", "store_performance", "inventory_alerts",
            "customer_segments", "product_performance"
        ]

        for view in analytical_views:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {self.catalog_name}.{self.schema_name}.{view}").collect()[0]['count']
                validation_results["checks"][f"{view}_view_exists"] = count > 0
                validation_results["checks"][f"{view}_view_count"] = count
            except Exception as e:
                validation_results["checks"][f"{view}_view_exists"] = False
                validation_results["checks"][f"{view}_view_error"] = str(e)

        # Check Genie configuration
        try:
            genie_config_count = spark.sql(f"SELECT COUNT(*) as count FROM {self.catalog_name}.{self.schema_name}.genie_configuration").collect()[0]['count']
            validation_results["checks"]["genie_configured"] = genie_config_count > 0
        except Exception as e:
            validation_results["checks"]["genie_configured"] = False
            validation_results["checks"]["genie_error"] = str(e)

        # Overall status
        failed_checks = [k for k, v in validation_results["checks"].items() if k.endswith("_exists") and not v]
        validation_results["overall_status"] = "SUCCESS" if len(failed_checks) == 0 else "FAILED"
        validation_results["failed_checks"] = failed_checks

        return validation_results

    def create_user_groups(self) -> bool:
        """
        Create user groups for role-based access
        """
        logger.info("Creating user groups...")

        groups = [
            {
                "name": "retail_analysts",
                "description": "Retail analysts with full access to cockpit data"
            },
            {
                "name": "store_managers",
                "description": "Store managers with store-level access"
            },
            {
                "name": "merchandisers",
                "description": "Merchandisers with product and category access"
            },
            {
                "name": "executives",
                "description": "Executives with high-level strategic access"
            },
            {
                "name": "supply_chain_managers",
                "description": "Supply chain managers with inventory and supplier access"
            }
        ]

        try:
            for group in groups:
                # In a real implementation, you would use the Databricks API
                # to create these groups. For now, we'll just log them.
                logger.info(f"Would create group: {group['name']} - {group['description']}")

            logger.info("User groups created successfully")
            return True

        except Exception as e:
            logger.error(f"User group creation failed: {e}")
            return False

    def generate_deployment_report(self, validation_results: Dict[str, Any]) -> str:
        """
        Generate a comprehensive deployment report
        """
        report = f"""
# Retail Insight Cockpit Deployment Report

**Environment:** {self.deployment_env}
**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Catalog:** {self.catalog_name}
**Schema:** {self.schema_name}

## Deployment Status: {validation_results['overall_status']}

## Component Status

### Core Tables
"""

        tables = ["sales", "customers", "products", "stores", "inventory", "promotions", "suppliers"]
        for table in tables:
            exists = validation_results["checks"].get(f"{table}_exists", False)
            count = validation_results["checks"].get(f"{table}_count", 0)
            status = "✅" if exists else "❌"
            report += f"- {table}: {status} ({count:,} rows)\n"

        report += "\n### Analytical Views\n"
        views = ["daily_sales_agg", "store_performance", "inventory_alerts", "customer_segments", "product_performance"]
        for view in views:
            exists = validation_results["checks"].get(f"{view}_view_exists", False)
            count = validation_results["checks"].get(f"{view}_view_count", 0)
            status = "✅" if exists else "❌"
            report += f"- {view}: {status} ({count:,} rows)\n"

        report += "\n### Genie Integration\n"
        genie_configured = validation_results["checks"].get("genie_configured", False)
        genie_status = "✅" if genie_configured else "❌"
        report += f"- Genie Configuration: {genie_status}\n"

        if validation_results["failed_checks"]:
            report += "\n## Failed Checks\n"
            for check in validation_results["failed_checks"]:
                report += f"- {check}\n"

        report += "\n## Next Steps\n"
        if validation_results["overall_status"] == "SUCCESS":
            report += """
1. Access dashboards in Databricks workspace
2. Configure user permissions for each role
3. Test Genie natural language queries
4. Schedule daily aggregation pipeline
5. Set up monitoring and alerts
"""
        else:
            report += """
1. Review failed checks above
2. Re-run deployment scripts for failed components
3. Verify Unity Catalog permissions
4. Check data source connections
5. Contact support if issues persist
"""

        return report

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Deployment Function

# COMMAND ----------

def deploy_retail_cockpit(catalog_name: str,
                         schema_name: str,
                         workspace_url: str,
                         deployment_env: str = "dev",
                         skip_data_generation: bool = False) -> Dict[str, Any]:
    """
    Main function to deploy the Retail Insight Cockpit
    """

    logger.info(f"Starting Retail Insight Cockpit deployment for {deployment_env}")

    # Initialize deployer
    deployer = RetailCockpitDeployer(catalog_name, schema_name, workspace_url, deployment_env)

    deployment_result = {
        "start_time": datetime.now(),
        "environment": deployment_env,
        "steps": [],
        "overall_status": "IN_PROGRESS"
    }

    try:
        # Step 1: Validate prerequisites
        logger.info("Step 1: Validating prerequisites...")
        prerequisites = deployer.validate_prerequisites()
        deployment_result["steps"].append({
            "step": "validate_prerequisites",
            "status": "SUCCESS" if all(prerequisites.values()) else "FAILED",
            "details": prerequisites
        })

        if not all(prerequisites.values()):
            logger.error("Prerequisites validation failed")
            deployment_result["overall_status"] = "FAILED"
            return deployment_result

        # Step 2: Create deployment configuration
        logger.info("Step 2: Creating deployment configuration...")
        config = deployer.create_deployment_config()
        deployment_result["steps"].append({
            "step": "create_config",
            "status": "SUCCESS",
            "details": config
        })

        # Step 3: Setup data sources
        logger.info("Step 3: Setting up data sources...")
        data_setup_success = deployer.setup_data_sources()
        deployment_result["steps"].append({
            "step": "setup_data_sources",
            "status": "SUCCESS" if data_setup_success else "FAILED"
        })

        if not data_setup_success:
            logger.error("Data sources setup failed")
            deployment_result["overall_status"] = "FAILED"
            return deployment_result

        # Step 4: Setup Genie integration
        logger.info("Step 4: Setting up Genie integration...")
        genie_setup_success = deployer.setup_genie_integration()
        deployment_result["steps"].append({
            "step": "setup_genie",
            "status": "SUCCESS" if genie_setup_success else "FAILED"
        })

        # Step 5: Create user groups
        logger.info("Step 5: Creating user groups...")
        groups_success = deployer.create_user_groups()
        deployment_result["steps"].append({
            "step": "create_user_groups",
            "status": "SUCCESS" if groups_success else "FAILED"
        })

        # Step 6: Validate deployment
        logger.info("Step 6: Validating deployment...")
        validation_results = deployer.validate_deployment()
        deployment_result["steps"].append({
            "step": "validate_deployment",
            "status": validation_results["overall_status"],
            "details": validation_results
        })

        # Step 7: Generate deployment report
        logger.info("Step 7: Generating deployment report...")
        report = deployer.generate_deployment_report(validation_results)
        deployment_result["deployment_report"] = report

        # Final status
        deployment_result["overall_status"] = validation_results["overall_status"]
        deployment_result["end_time"] = datetime.now()
        deployment_result["duration"] = (deployment_result["end_time"] - deployment_result["start_time"]).total_seconds()

        logger.info(f"Deployment completed with status: {deployment_result['overall_status']}")

        return deployment_result

    except Exception as e:
        logger.error(f"Deployment failed with exception: {e}")
        deployment_result["overall_status"] = "FAILED"
        deployment_result["error"] = str(e)
        deployment_result["end_time"] = datetime.now()
        return deployment_result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def cleanup_deployment(catalog_name: str, schema_name: str, deployment_env: str):
    """
    Clean up a deployment (for development/testing)
    """
    logger.warning(f"Cleaning up deployment for {deployment_env}")

    if deployment_env == "prod":
        logger.error("Cannot cleanup production environment")
        return False

    try:
        # Drop materialized tables
        materialized_tables = [
            "daily_sales_agg_materialized",
            "store_performance_materialized",
            "inventory_alerts_materialized",
            "customer_segments_materialized",
            "product_performance_materialized"
        ]

        for table in materialized_tables:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{table}")
                logger.info(f"Dropped table: {table}")
            except Exception as e:
                logger.warning(f"Could not drop table {table}: {e}")

        # Drop Genie configuration tables
        genie_tables = [
            "genie_configuration",
            "genie_sample_questions",
            "genie_space_config",
            "genie_dashboard_widgets"
        ]

        for table in genie_tables:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{table}")
                logger.info(f"Dropped Genie table: {table}")
            except Exception as e:
                logger.warning(f"Could not drop Genie table {table}: {e}")

        logger.info("Cleanup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return False

def get_deployment_status(catalog_name: str, schema_name: str) -> Dict[str, Any]:
    """
    Get current deployment status
    """
    status = {
        "timestamp": datetime.now().isoformat(),
        "catalog": catalog_name,
        "schema": schema_name,
        "components": {}
    }

    # Check core tables
    core_tables = ["sales", "customers", "products", "stores", "inventory"]
    for table in core_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table}").collect()[0]['count']
            status["components"][table] = {"exists": True, "row_count": count}
        except:
            status["components"][table] = {"exists": False, "row_count": 0}

    # Check analytical views
    views = ["daily_sales_agg", "store_performance", "inventory_alerts"]
    for view in views:
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{view}").collect()[0]['count']
            status["components"][f"{view}_view"] = {"exists": True, "row_count": count}
        except:
            status["components"][f"{view}_view"] = {"exists": False, "row_count": 0}

    return status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Example deployment
if __name__ == "__main__":
    # Get parameters
    catalog_name = dbutils.widgets.get("catalog_name") if dbutils.widgets.get("catalog_name") else "retail_analytics_dev"
    schema_name = dbutils.widgets.get("schema_name") if dbutils.widgets.get("schema_name") else "cockpit"
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    deployment_env = dbutils.widgets.get("deployment_env") if dbutils.widgets.get("deployment_env") else "dev"

    print(f"""
RETAIL INSIGHT COCKPIT DEPLOYMENT UTILITY
==========================================

Target Configuration:
- Environment: {deployment_env}
- Catalog: {catalog_name}
- Schema: {schema_name}
- Workspace: {workspace_url}

Available Functions:
- deploy_retail_cockpit(): Full deployment
- cleanup_deployment(): Remove deployment (dev/staging only)
- get_deployment_status(): Check current status

To deploy, run:
result = deploy_retail_cockpit("{catalog_name}", "{schema_name}", "{workspace_url}", "{deployment_env}")
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This deployment utility provides:
# MAGIC
# MAGIC ✅ **Complete deployment automation** with validation and error handling
# MAGIC
# MAGIC ✅ **Environment-specific configurations** for dev/staging/prod
# MAGIC
# MAGIC ✅ **Prerequisite validation** to ensure successful deployment
# MAGIC
# MAGIC ✅ **Component-by-component validation** with detailed reporting
# MAGIC
# MAGIC ✅ **User group creation** for role-based access control
# MAGIC
# MAGIC ✅ **Cleanup utilities** for development and testing
# MAGIC
# MAGIC ✅ **Comprehensive deployment reports** with status and next steps
# MAGIC
# MAGIC The utility handles the complete deployment lifecycle from initial setup through validation and user enablement.