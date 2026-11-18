"""deposit_utils: shared helpers for the Deposit Studies workspace.

Modules
-------
study     Study context (paths, versioned table names, params) for one version folder
sqlrun    {{token}} rendering and multi-statement SQL execution for Databricks
folders   Folder scaffolding: create / clone study versions, index, links notebook
betas     Beta engine: WAR pivot, fed funds, rate cycles, endpoint / linear / poly betas
config    PRODUCT_CONFIG loader, validator, describe / resolve helpers
compare   Version-to-version comparison (inputs, mappings, classifications, betas)
promote   Controlled promotion of sandbox tables to finance.pricing_analytics
"""
__version__ = "1.0.0"
