import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import {
 Activity, AlertCircle, AlertTriangle, ArrowDownRight, ArrowRight, ArrowUpDown,
 ArrowUpRight, BarChart3, Bell, BookOpen, Box, Check, CheckCircle2, ChevronDown,
 ChevronLeft, ChevronRight, Clock, Cloud, Code2, Cpu, Database, Download, Edit3,
 Eye, FileCode, FileSpreadsheet, FileText, Filter, FolderKanban, GitBranch,
 Globe, HardDrive, HelpCircle, History, Info, Layers, LayoutDashboard, LineChart as LineChartIcon,
 Link2, ListFilter, Loader2, Lock, Moon, MoreHorizontal, Network, Package, Pause,
 PieChart as PieChartIcon, Play, Plus, RefreshCw, RotateCcw, Save, Search, Server,
 Settings as SettingsIcon, ShieldAlert, ShieldCheck, Sliders, Sparkles, Square,
 Sun, Table as TableIcon, Terminal, Trash2, TrendingUp, Upload, User, Users,
 Workflow, Wrench, X, XCircle, Zap
} from 'lucide-react';
import {
 ResponsiveContainer, AreaChart, Area, BarChart, Bar, LineChart, Line,
 PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend
} from 'recharts';

// ============================================================================
// 1. CORE TYPES & INTERFACES
// ============================================================================

type NavPageId =
 | 'dashboard'
 | 'activity'
 | 'sources'
 | 'ingestion'
 | 'builder'
 | 'monitor'
 | 'quality'
 | 'explorer'
 | 'analytics'
 | 'reports'
 | 'catalog'
 | 'architecture'
 | 'integrations'
 | 'logs'
 | 'settings';

type ExecutionStatus = 'Draft' | 'Queued' | 'Running' | 'Succeeded' | 'Failed' | 'Cancelled';
type StageStatus = 'Pending' | 'Running' | 'Succeeded' | 'Failed' | 'Skipped';

type NodeType =
 | 'SOURCE'
 | 'INGEST'
 | 'VALIDATE'
 | 'DEDUPLICATE'
 | 'TRANSFORM'
 | 'FILTER'
 | 'AGGREGATE'
 | 'CURATED_STORAGE'
 | 'ANALYTICS';

interface DataRow {
 _rowId?: number;
 order_id: string;
 order_date: string;
 product_name: string;
 category: string;
 quantity: number | string | null;
 unit_price: number | string | null;
 region: string | null;
 customer_segment: string | null;
 payment_method?: string;
 revenue?: number;
 order_year?: number;
 order_month?: string;
 order_day?: number;
 deal_size_tier?: string;
 [key: string]: any;
}

interface ColumnSchema {
 name: string;
 inferredType: 'string' | 'number' | 'date' | 'boolean';
 nullable: boolean;
 nullCount: number;
 uniqueCount: number;
 sampleValues: string[];
 min?: number | string;
 max?: number | string;
 mean?: number;
}

interface IngestionConfig {
 sourceName: string;
 sourceType: 'CSV Upload' | 'JSON Upload' | 'Synthetic Dataset' | 'SQL Read Replica' | 'Azure ADLS Gen2';
 datasetName: string;
 destinationLayer: 'raw_bronze' | 'staging_silver' | 'curated_gold';
 schemaStrategy: 'Infer & Evolve' | 'Strict Enforcement' | 'Quarantine Invalid';
 duplicateHandling: 'Remove Exact & Key Duplicates' | 'Remove Exact Only' | 'Retain All';
 nullHandling: 'Impute Median / Defaults' | 'Drop Rows with Nulls' | 'Retain Nulls';
 dateParsingRule: 'ISO YYYY-MM-DD (Drop Invalid)' | 'Coerce Invalid to Fallback Date';
 columnMappings: { sourceCol: string; targetCol: string; targetType: string; required: boolean }[];
}

interface TransformationRuleConfig {
 deduplicateExact: boolean;
 deduplicateBusinessKey: string;
 normalizeWhitespace: boolean;
 standardizeCategories: boolean;
 imputeMissingValues: boolean;
 dropInvalidDates: boolean;
 fixNegativeNumbers: boolean;
 deriveRevenue: boolean;
 deriveDateParts: boolean;
 deriveDealTier: boolean;
 filterEnabled: boolean;
 filterColumn: string;
 filterOperator: 'gt' | 'lt' | 'contains' | 'equals' | 'not_null' | 'date_range';
 filterValue: string;
 filterValueEnd?: string;
 aggregateGroupBy: string;
 aggregateMetric: 'sum' | 'mean' | 'count' | 'min' | 'max';
 simulateStrictSchemaFailure?: boolean;
}

interface TransformationStepResult {
 id: string;
 name: string;
 type: 'DEDUPLICATE' | 'CLEAN' | 'VALIDATE' | 'FILTER' | 'FEATURE_ENGINEERING' | 'TYPE_CONVERSION' | 'AGGREGATE';
 inputColumns: string[];
 configSummary: string;
 status: StageStatus;
 inputRows: number;
 outputRows: number;
 removedRows: number;
 modifiedRows: number;
 durationMs: number;
 errorInfo?: string;
 sampleBefore: DataRow[];
 sampleAfter: DataRow[];
}

interface QualityRuleCheck {
 id: string;
 name: string;
 dimension: 'Completeness' | 'Uniqueness' | 'Validity' | 'Consistency' | 'Schema Conformance';
 description: string;
 passedCount: number;
 failedCount: number;
 totalCount: number;
 passRate: number;
 status: 'Passed' | 'Warning' | 'Failed';
 remediation: string;
 affectedRows: DataRow[];
}

interface QualityReport {
 overallScore: number;
 completenessScore: number;
 uniquenessScore: number;
 validityScore: number;
 consistencyScore: number;
 totalRows: number;
 validRows: number;
 invalidRows: number;
 duplicateRows: number;
 exactDuplicateRows: number;
 inconsistentRows: number;
 nullCountsByColumn: Record<string, number>;
 checks: QualityRuleCheck[];
}

interface DatasetEntity {
 id: string;
 name: string;
 sourceName: string;
 sourceType: IngestionConfig['sourceType'];
 createdAt: string;
 updatedAt: string;
 rawRows: DataRow[];
 curatedRows: DataRow[];
 quarantinedRows: DataRow[];
 rawQuality: QualityReport;
 curatedQuality: QualityReport;
 schema: ColumnSchema[];
 curatedSchema: ColumnSchema[];
 status: 'Raw Ingested' | 'Curated & Verified' | 'Validation Failed';
 latestExecutionId: string;
 ingestionConfig: IngestionConfig;
 transformationSteps: TransformationStepResult[];
}

interface PipelineNodeData {
 id: string;
 type: NodeType;
 label: string;
 subtitle: string;
 azureService: string;
 x: number;
 y: number;
 config: Record<string, any>;
}

interface PipelineEdgeData {
 id: string;
 source: string;
 target: string;
}

interface PipelineDefinition {
 id: string;
 name: string;
 description: string;
 datasetId: string;
 nodes: PipelineNodeData[];
 edges: PipelineEdgeData[];
 transformConfig: TransformationRuleConfig;
 lastModified: string;
}

interface StageExecutionRecord {
 stageId: string;
 stageName: string;
 nodeType: NodeType;
 azureService: string;
 status: StageStatus;
 inputRows: number;
 outputRows: number;
 durationMs: number;
 logs: string[];
 errorMessage?: string;
}

interface PipelineExecutionRecord {
 executionId: string;
 pipelineId: string;
 pipelineName: string;
 datasetId: string;
 datasetName: string;
 triggerType: 'Manual' | 'File Upload' | 'Scheduled ADF Trigger' | 'Auto-Retry';
 startTime: string;
 completionTime: string;
 actualComputeMs: number;
 totalDurationMs: number;
 status: ExecutionStatus;
 currentStageIndex: number;
 inputRowCount: number;
 outputRowCount: number;
 retryCount: number;
 errorMessage?: string;
 stages: StageExecutionRecord[];
}

interface SavedAnalyticsReport {
 id: string;
 title: string;
 datasetId: string;
 datasetName: string;
 createdAt: string;
 notes: string;
 filters: {
  dateRange: string;
  category: string;
  region: string;
  segment: string;
  groupBy: string;
  metric: string;
 };
 summaryMetrics: {
  totalRevenue: number;
  totalOrders: number;
  avgOrderValue: number;
  topCategory: string;
  qualityScore: number;
 };
}

interface PlatformAlert {
 id: string;
 type: 'Failed Ingestion' | 'Quality Threshold Violation' | 'Schema Mismatch' | 'Transformation Failure' | 'Missing Required Columns';
 severity: 'critical' | 'warning' | 'info';
 title: string;
 description: string;
 datasetId: string;
 executionId?: string;
 timestamp: string;
 acknowledged: boolean;
}

interface ActivityEvent {
 id: string;
 timestamp: string;
 actor: string;
 category: 'Pipeline' | 'Ingestion' | 'Quality' | 'Analytics' | 'Architecture';
 title: string;
 detail: string;
 targetPage: NavPageId;
 status: 'success' | 'warning' | 'error' | 'info';
}

interface SystemLogEntry {
 id: string;
 timestamp: string;
 service: 'ADF-Ingestion' | 'ADLS-Gen2' | 'Databricks-Spark' | 'Quality-Engine' | 'Synapse-SQL';
 level: 'INFO' | 'WARN' | 'ERROR';
 executionId: string;
 message: string;
}

// ============================================================================
// 2. INITIAL SYNTHETIC DATASETS (WITH REALISTIC DATA ENGINEERING EDGE CASES)
// ============================================================================

const INITIAL_ECOMMERCE_RAW_ROWS: DataRow[] = [
 { order_id: 'ORD-1001', order_date: '2026-08-01', product_name: 'Surface Pro 11 Enterprise', category: 'Electronics', quantity: 4, unit_price: 1299.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1002', order_date: '2026-08-02', product_name: 'Azure Kinect DK Sensor', category: '  electronics ', quantity: 10, unit_price: 399.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1003', order_date: '2026-08-03', product_name: 'Ergonomic Mesh Task Chair', category: 'Office Furniture', quantity: 15, unit_price: 349.50, region: 'Europe', customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1004', order_date: '2026-08-04', product_name: 'Thunderbolt 4 Dock G5', category: 'ELECTRONICS', quantity: 25, unit_price: 279.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1005', order_date: '2026-08-05', product_name: 'Standing Desk Dual Motor', category: 'office furniture', quantity: 8, unit_price: 620.00, region: 'North America', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'ORD-1006', order_date: '2026-08-06', product_name: 'Power BI Premium Per User (Annual)', category: 'Software Licenses', quantity: 50, unit_price: 240.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1007', order_date: '2026-08-07', product_name: 'Noise-Canceling Conferencing Headset', category: 'Accessories', quantity: 30, unit_price: 189.00, region: 'Latin America', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1008', order_date: '2026-08-08', product_name: '4K UltraSharp 32 Monitor', category: 'Electronics', quantity: 12, unit_price: 749.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 // Exact duplicate of ORD-1008
 { order_id: 'ORD-1008', order_date: '2026-08-08', product_name: '4K UltraSharp 32 Monitor', category: 'Electronics', quantity: 12, unit_price: 749.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1009', order_date: '2026-08-09', product_name: 'Rackmount UPS 3000VA', category: 'Cloud Hardware', quantity: 3, unit_price: 1450.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1010', order_date: '2026-08-10', product_name: 'Mechanical Wireless Keyboard', category: '  accessories', quantity: 40, unit_price: 129.00, region: 'Europe', customer_segment: 'SMB', payment_method: 'Credit Card' },
 // Edge case: Missing customer_segment + lowercase category
 { order_id: 'ORD-1011', order_date: '2026-08-11', product_name: 'GitHub Enterprise Seat Pack', category: 'software licenses', quantity: 35, unit_price: 252.00, region: 'North America', customer_segment: null, payment_method: 'Wire Transfer' },
 // Edge case: Invalid date format
 { order_id: 'ORD-1012', order_date: '2026-13-45', product_name: 'Smart Room Bar Camera', category: 'Electronics', quantity: 5, unit_price: 999.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1013', order_date: '2026-08-13', product_name: 'Modular Acoustic Desk Partition', category: 'Office Furniture', quantity: 20, unit_price: 175.00, region: 'North America', customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1014', order_date: '2026-08-14', product_name: '10GbE Managed Switch 24-Port', category: 'cloud_hardware', quantity: 6, unit_price: 1120.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 // Edge case: Negative quantity
 { order_id: 'ORD-1015', order_date: '2026-08-15', product_name: 'USB-C Multiport Travel Hub', category: 'Accessories', quantity: -4, unit_price: 79.00, region: 'North America', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'ORD-1016', order_date: '2026-08-16', product_name: 'Surface Laptop Studio 2', category: 'Electronics', quantity: 7, unit_price: 2199.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 // Exact duplicate of ORD-1016
 { order_id: 'ORD-1016', order_date: '2026-08-16', product_name: 'Surface Laptop Studio 2', category: 'Electronics', quantity: 7, unit_price: 2199.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1017', order_date: '2026-08-18', product_name: 'Azure Databricks Commit Unit Pack', category: 'Software Licenses', quantity: 10, unit_price: 950.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 // Edge case: Missing unit_price
 { order_id: 'ORD-1018', order_date: '2026-08-19', product_name: 'Dual Monitor Articulating Arm', category: 'Accessories', quantity: 18, unit_price: null, region: 'Latin America', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1019', order_date: '2026-08-21', product_name: 'Enterprise NAS Storage Array 64TB', category: 'Cloud Hardware', quantity: 2, unit_price: 4200.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1020', order_date: '2026-08-22', product_name: 'Executive Conference Table 12ft', category: '  Office Furniture ', quantity: 2, unit_price: 1850.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Invoice Net-30' },
 // Business key duplicate of ORD-1020 with updated quantity
 { order_id: 'ORD-1020', order_date: '2026-08-22', product_name: 'Executive Conference Table 12ft', category: 'Office Furniture', quantity: 3, unit_price: 1850.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1021', order_date: '2026-08-24', product_name: 'SQL Server Enterprise Core License', category: 'SOFTWARE LICENSES', quantity: 8, unit_price: 1425.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1022', order_date: '2026-08-26', product_name: 'Wireless Presentation Clicker Pro', category: 'Accessories', quantity: 45, unit_price: 65.00, region: 'North America', customer_segment: 'SMB', payment_method: 'Credit Card' },
 // Edge case: Missing region
 { order_id: 'ORD-1023', order_date: '2026-08-28', product_name: 'OLED Portable Field Monitor', category: 'electronics', quantity: 9, unit_price: 460.00, region: null, customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1024', order_date: '2026-09-01', product_name: 'AI Workstation GPU Server Node', category: 'Cloud Hardware', quantity: 2, unit_price: 6800.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1025', order_date: '2026-09-03', product_name: 'Lumbar Support Ergonomic Footrest', category: 'Office Furniture', quantity: 55, unit_price: 58.00, region: 'Europe', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'ORD-1026', order_date: '2026-09-05', product_name: 'Defender for Cloud Server Plan', category: 'Software Licenses', quantity: 40, unit_price: 180.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1027', order_date: '2026-09-07', product_name: 'Precision Wireless Trackpad', category: 'ACCESSORIES', quantity: 28, unit_price: 115.00, region: 'Asia Pacific', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 // Edge case: Invalid date string
 { order_id: 'ORD-1028', order_date: 'INVALID_DATE', product_name: 'Fiber Optic Patch Cable 10m Pack', category: 'Cloud Hardware', quantity: 20, unit_price: 95.00, region: 'Latin America', customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1029', order_date: '2026-09-10', product_name: 'Surface Hub 3 50-inch Collaboration', category: 'Electronics', quantity: 3, unit_price: 4499.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1030', order_date: '2026-09-12', product_name: 'Mobile Locking Pedestal File', category: 'Office Furniture', quantity: 16, unit_price: 240.00, region: 'Europe', customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 // Edge case: Negative unit price
 { order_id: 'ORD-1031', order_date: '2026-09-14', product_name: 'HDMI 2.1 Braided Cable 3m', category: 'Accessories', quantity: 60, unit_price: -24.00, region: 'North America', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'ORD-1032', order_date: '2026-09-16', product_name: 'Microsoft Fabric Capacity F16 Pack', category: 'Software Licenses', quantity: 6, unit_price: 1920.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1033', order_date: '2026-09-18', product_name: 'Edge Gateway Industrial Appliance', category: 'Cloud Hardware', quantity: 8, unit_price: 890.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1034', order_date: '2026-09-20', product_name: '27-inch QHD IPS Designer Display', category: 'Electronics', quantity: 14, unit_price: 499.00, region: 'Latin America', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 // Exact duplicate of ORD-1034
 { order_id: 'ORD-1034', order_date: '2026-09-20', product_name: '27-inch QHD IPS Designer Display', category: 'Electronics', quantity: 14, unit_price: 499.00, region: 'Latin America', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1035', order_date: '2026-09-22', product_name: 'Acoustic Phone Booth Pod', category: 'Office Furniture', quantity: 2, unit_price: 3400.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 // Edge case: Missing customer_segment
 { order_id: 'ORD-1036', order_date: '2026-09-25', product_name: 'Encrypted Hardware Security Key', category: 'Accessories', quantity: 80, unit_price: 55.00, region: 'Europe', customer_segment: null, payment_method: 'Corporate Card' },
 { order_id: 'ORD-1037', order_date: '2026-09-28', product_name: 'Azure Purview Governance Suite', category: 'Software Licenses', quantity: 12, unit_price: 680.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1038', order_date: '2026-10-01', product_name: 'NVMe All-Flash Cache Blade', category: 'cloud hardware', quantity: 4, unit_price: 2350.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1039', order_date: '2026-10-03', product_name: 'Webcam 4K AI Auto-Framing', category: 'Electronics', quantity: 22, unit_price: 199.00, region: 'Europe', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1040', order_date: '2026-10-05', product_name: 'Collaborative Whiteboard Easel', category: 'Office Furniture', quantity: 11, unit_price: 295.00, region: 'Latin America', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'ORD-1041', order_date: '2026-10-07', product_name: 'Visual Studio Enterprise Annual', category: 'Software Licenses', quantity: 15, unit_price: 1199.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1042', order_date: '2026-10-09', product_name: 'Laptop Privacy Filter 15-inch', category: 'Accessories', quantity: 65, unit_price: 48.00, region: 'Europe', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'ORD-1043', order_date: '2026-10-11', product_name: 'Redundant Power Distribution Unit', category: 'Cloud Hardware', quantity: 9, unit_price: 540.00, region: 'North America', customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1044', order_date: '2026-10-13', product_name: 'Surface Pro Flex Keyboard Pen Bundle', category: 'Electronics', quantity: 19, unit_price: 449.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1045', order_date: '2026-10-15', product_name: 'Height-Adjustable Drafting Stool', category: 'Office Furniture', quantity: 14, unit_price: 280.00, region: 'North America', customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 { order_id: 'ORD-1046', order_date: '2026-10-17', product_name: 'Azure Synapse Dedicated SQL Pack', category: 'Software Licenses', quantity: 5, unit_price: 2600.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'ORD-1047', order_date: '2026-10-19', product_name: 'Desktop GaN Charging Station 140W', category: 'Accessories', quantity: 32, unit_price: 110.00, region: 'North America', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'ORD-1048', order_date: '2026-10-20', product_name: 'Enterprise Firewall Security Gateway', category: 'Cloud Hardware', quantity: 5, unit_price: 1780.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' }
];

const SECONDARY_MARKETPLACE_RAW_ROWS: DataRow[] = [
 { order_id: 'MKT-2001', order_date: '2026-08-04', product_name: 'Azure OpenAI Provisioned Throughput', category: 'Software Licenses', quantity: 4, unit_price: 3200.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'MKT-2002', order_date: '2026-08-09', product_name: 'AKS Cluster Guard Sensor Pack', category: '  cloud_hardware ', quantity: 12, unit_price: 640.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Corporate Card' },
 { order_id: 'MKT-2002', order_date: '2026-08-09', product_name: 'AKS Cluster Guard Sensor Pack', category: '  cloud_hardware ', quantity: 12, unit_price: 640.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Corporate Card' },
 { order_id: 'MKT-2003', order_date: '2026-08-17', product_name: 'ExpressRoute Direct 100Gbps Port', category: 'Cloud Hardware', quantity: 2, unit_price: 5400.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'MKT-2004', order_date: '2026-08-25', product_name: 'Defender Threat Intelligence Feed', category: 'software licenses', quantity: 18, unit_price: 410.00, region: null, customer_segment: 'Mid-Market', payment_method: 'Invoice Net-30' },
 { order_id: 'MKT-2005', order_date: '2026-09-02', product_name: 'HoloLens 2 Industrial Suite', category: 'ELECTRONICS', quantity: 6, unit_price: 3500.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'MKT-2006', order_date: '2026-09-11', product_name: 'Smart Rack PDU Telemetry Unit', category: 'Accessories', quantity: -5, unit_price: 210.00, region: 'Europe', customer_segment: 'Mid-Market', payment_method: 'Corporate Card' },
 { order_id: 'MKT-2007', order_date: '2026-09-19', product_name: 'Cosmos DB Autoscale Reserved Pack', category: 'Software Licenses', quantity: 10, unit_price: 1480.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'MKT-2008', order_date: '2026-99-99', product_name: 'Legacy Connector Adapter', category: 'Accessories', quantity: 15, unit_price: 95.00, region: 'Latin America', customer_segment: 'SMB', payment_method: 'Credit Card' },
 { order_id: 'MKT-2009', order_date: '2026-10-02', product_name: 'Azure Stack HCI Edge Server', category: 'Cloud Hardware', quantity: 3, unit_price: 4950.00, region: 'Europe', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'MKT-2010', order_date: '2026-10-08', product_name: 'Executive Sit-Stand Command Console', category: 'Office Furniture', quantity: 5, unit_price: 1190.00, region: 'North America', customer_segment: null, payment_method: 'Invoice Net-30' },
 { order_id: 'MKT-2011', order_date: '2026-10-14', product_name: 'Log Analytics Dedicated Cluster 500GB', category: 'Software Licenses', quantity: 8, unit_price: 1750.00, region: 'Asia Pacific', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' },
 { order_id: 'MKT-2012', order_date: '2026-10-18', product_name: 'Surface Studio 2+ Executive Bundle', category: 'Electronics', quantity: 4, unit_price: 4299.00, region: 'North America', customer_segment: 'Enterprise', payment_method: 'Wire Transfer' }
];

const SAMPLE_UPLOAD_CSV_TEXT = `order_id,order_date,product_name,category,quantity,unit_price,region,customer_segment,payment_method
ORD-3001,2026-09-01,Azure Arc Enabled Server Pack,Software Licenses,12,850.00,North America,Enterprise,Wire Transfer
ORD-3002,2026-09-03,Surface Laptop 6 Business,  electronics ,8,1599.00,Europe,Mid-Market,Corporate Card
ORD-3002,2026-09-03,Surface Laptop 6 Business,  electronics ,8,1599.00,Europe,Mid-Market,Corporate Card
ORD-3003,2026-09-06,Modular Server Rack 42U,cloud_hardware,3,2400.00,Asia Pacific,Enterprise,Invoice Net-30
ORD-3004,2026-14-99,Corrupted Date Sensor Kit,Accessories,20,125.00,North America,SMB,Credit Card
ORD-3005,2026-09-12,Acoustic Studio Panel Set,office furniture,-6,310.00,Europe,,Invoice Net-30
ORD-3006,2026-09-18,Synapse Dedicated SQL Pool Unit,SOFTWARE LICENSES,5,3100.00,North America,Enterprise,Wire Transfer
ORD-3007,2026-09-24,4K PTZ Conference Optics,Electronics,10,,Latin America,Mid-Market,Corporate Card
ORD-3008,2026-10-04,Direct Connect Fiber Transceiver,Cloud Hardware,25,380.00,North America,Enterprise,Wire Transfer
ORD-3009,2026-10-11,Ergonomic Executive Task Chair,Office Furniture,14,490.00,Europe,Mid-Market,Corporate Card
ORD-3010,2026-10-18,GitHub Copilot Enterprise Pack,Software Licenses,45,390.00,North America,Enterprise,Wire Transfer`;

// ============================================================================
// 3. DATA PARSING, QUALITY ENGINE & TRANSFORMATION ENGINE
// ============================================================================

function isValidISODate(dateStr: any): boolean {
 if (typeof dateStr !== 'string') return false;
 const trimmed = dateStr.trim();
 const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimmed);
 if (!match) return false;
 const year = parseInt(match[1], 10);
 const month = parseInt(match[2], 10);
 const day = parseInt(match[3], 10);
 if (year < 1990 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) return false;
 const d = new Date(Date.UTC(year, month - 1, day));
 return d.getUTCFullYear() === year && d.getUTCMonth() === month - 1 && d.getUTCDate() === day;
}

function normalizeCategoryLabel(rawCat: any): string {
 if (rawCat === null || rawCat === undefined) return 'Uncategorized';
 const cleaned = String(rawCat).trim().replace(/_/g, ' ').replace(/\s+/g, ' ');
 if (!cleaned) return 'Uncategorized';
 return cleaned
  .toLowerCase()
  .split(' ')
  .map(w => w.charAt(0).toUpperCase() + w.slice(1))
  .join(' ');
}

function inferDatasetSchema(rows: DataRow[]): ColumnSchema[] {
 if (!rows || rows.length === 0) return [];
 const colSet = new Set<string>();
 rows.forEach(r => Object.keys(r).forEach(k => {
  if (k !== '_rowId') colSet.add(k);
 }));
 const columns = Array.from(colSet);

 return columns.map(col => {
  let nullCount = 0;
  const nonNullValues: any[] = [];
  const uniqueSet = new Set<string>();

  rows.forEach(r => {
   const val = r[col];
   if (val === null || val === undefined || String(val).trim() === '') {
    nullCount++;
   } else {
    nonNullValues.push(val);
    uniqueSet.add(String(val).trim());
   }
  });

  let inferredType: ColumnSchema['inferredType'] = 'string';
  if (nonNullValues.length > 0) {
   const numMatches = nonNullValues.filter(v => typeof v === 'number' || (!isNaN(Number(v)) && String(v).trim() !== '')).length;
   const dateMatches = nonNullValues.filter(v => isValidISODate(String(v)) || col.toLowerCase().includes('date')).length;
   if (col.toLowerCase().includes('date') || dateMatches / nonNullValues.length > 0.7) {
    inferredType = 'date';
   } else if (numMatches / nonNullValues.length > 0.8 && !col.toLowerCase().includes('id')) {
    inferredType = 'number';
   }
  }

  const schemaItem: ColumnSchema = {
   name: col,
   inferredType,
   nullable: nullCount > 0,
   nullCount,
   uniqueCount: uniqueSet.size,
   sampleValues: Array.from(uniqueSet).slice(0, 4)
  };

  if (inferredType === 'number') {
   const nums = nonNullValues.map(v => Number(v)).filter(n => !isNaN(n));
   if (nums.length > 0) {
    schemaItem.min = Math.min(...nums);
    schemaItem.max = Math.max(...nums);
    schemaItem.mean = Number((nums.reduce((a, b) => a + b, 0) / nums.length).toFixed(2));
   }
  } else if (nonNullValues.length > 0) {
   const sorted = [...uniqueSet].sort();
   schemaItem.min = sorted[0];
   schemaItem.max = sorted[sorted.length - 1];
  }

  return schemaItem;
 });
}

function evaluateDataQuality(
 rows: DataRow[],
 requiredCols: string[] = ['order_id', 'order_date', 'product_name', 'category', 'quantity', 'unit_price', 'region', 'customer_segment']
): QualityReport {
 const totalRows = rows.length;
 if (totalRows === 0) {
  return {
   overallScore: 0,
   completenessScore: 0,
   uniquenessScore: 0,
   validityScore: 0,
   consistencyScore: 0,
   totalRows: 0,
   validRows: 0,
   invalidRows: 0,
   duplicateRows: 0,
   exactDuplicateRows: 0,
   inconsistentRows: 0,
   nullCountsByColumn: {},
   checks: []
  };
 }

 // 1. Completeness
 const nullCountsByColumn: Record<string, number> = {};
 requiredCols.forEach(c => { nullCountsByColumn[c] = 0; });
 const rowsWithNulls: DataRow[] = [];

 rows.forEach(r => {
  let rowHasNull = false;
  requiredCols.forEach(col => {
   const val = r[col];
   if (val === null || val === undefined || String(val).trim() === '') {
    nullCountsByColumn[col] = (nullCountsByColumn[col] || 0) + 1;
    rowHasNull = true;
   }
  });
  if (rowHasNull) rowsWithNulls.push(r);
 });

 const totalCells = totalRows * requiredCols.length;
 const totalNullCells = Object.values(nullCountsByColumn).reduce((a, b) => a + b, 0);
 const completenessScore = Number((((totalCells - totalNullCells) / totalCells) * 100).toFixed(1));

 // 2. Uniqueness (Exact duplicates + Business Key order_id duplicates)
 const seenSignatures = new Set<string>();
 const seenOrderIds = new Set<string>();
 const exactDupRows: DataRow[] = [];
 const keyDupRows: DataRow[] = [];

 rows.forEach(r => {
  const sig = JSON.stringify({
   order_id: r.order_id,
   order_date: r.order_date,
   product_name: r.product_name,
   category: r.category,
   quantity: r.quantity,
   unit_price: r.unit_price,
   region: r.region,
   customer_segment: r.customer_segment
  });
  if (seenSignatures.has(sig)) {
   exactDupRows.push(r);
  } else {
   seenSignatures.add(sig);
  }

  const oid = String(r.order_id || '').trim();
  if (oid) {
   if (seenOrderIds.has(oid)) {
    keyDupRows.push(r);
   } else {
    seenOrderIds.add(oid);
   }
  }
 });

 const uniquenessScore = Number((((totalRows - keyDupRows.length) / totalRows) * 100).toFixed(1));

 // 3. Validity (Date validity + positive numeric quantity & unit_price)
 const invalidDateRows: DataRow[] = [];
 const invalidNumericRows: DataRow[] = [];
 const invalidRowsSet = new Set<DataRow>();

 rows.forEach(r => {
  if (!isValidISODate(r.order_date)) {
   invalidDateRows.push(r);
   invalidRowsSet.add(r);
  }
  const q = Number(r.quantity);
  const p = Number(r.unit_price);
  const qtyInvalid = r.quantity === null || r.quantity === undefined || String(r.quantity).trim() === '' || isNaN(q) || q <= 0;
  const priceInvalid = r.unit_price === null || r.unit_price === undefined || String(r.unit_price).trim() === '' || isNaN(p) || p <= 0;
  if (qtyInvalid || priceInvalid) {
   invalidNumericRows.push(r);
   invalidRowsSet.add(r);
  }
 });

 const invalidRowsCount = invalidRowsSet.size;
 const validityScore = Number((((totalRows - invalidRowsCount) / totalRows) * 100).toFixed(1));

 // 4. Consistency (Standardized category labels without leading/trailing whitespace or underscores)
 const canonicalCategories = new Set(['Electronics', 'Office Furniture', 'Software Licenses', 'Accessories', 'Cloud Hardware']);
 const inconsistentCatRows: DataRow[] = [];

 rows.forEach(r => {
  const rawCat = String(r.category ?? '');
  const normalized = normalizeCategoryLabel(rawCat);
  if (rawCat !== normalized || !canonicalCategories.has(rawCat)) {
   inconsistentCatRows.push(r);
  }
 });

 const consistencyScore = Number((((totalRows - inconsistentCatRows.length) / totalRows) * 100).toFixed(1));

 // Transparent Weighted Quality Score Formula:
 // 30% Completeness + 25% Validity + 25% Uniqueness + 20% Consistency
 const overallScore = Number(
  (
   0.30 * completenessScore +
   0.25 * validityScore +
   0.25 * uniquenessScore +
   0.20 * consistencyScore
  ).toFixed(1)
 );

 const allFlaggedRows = new Set<DataRow>([
  ...rowsWithNulls,
  ...keyDupRows,
  ...Array.from(invalidRowsSet),
  ...inconsistentCatRows
 ]);
 const validRows = totalRows - allFlaggedRows.size;

 const presentColumns = rows.length > 0 ? Object.keys(rows[0]) : [];
 const missingReqCols = requiredCols.filter(c => !presentColumns.includes(c));

 const checks: QualityRuleCheck[] = [
  {
   id: 'chk-completeness',
   name: 'Required Field Non-Null Check',
   dimension: 'Completeness',
   description: 'Verifies that all required schema columns contain non-null, non-empty values.',
   passedCount: totalRows - rowsWithNulls.length,
   failedCount: rowsWithNulls.length,
   totalCount: totalRows,
   passRate: Number((((totalRows - rowsWithNulls.length) / totalRows) * 100).toFixed(1)),
   status: rowsWithNulls.length === 0 ? 'Passed' : 'Warning',
   remediation: 'Enable "Impute Median / Defaults" or "Drop Rows with Nulls" in the Transformation Engine.',
   affectedRows: rowsWithNulls
  },
  {
   id: 'chk-exact-dup',
   name: 'Exact Record Deduplication Check',
   dimension: 'Uniqueness',
   description: 'Detects 100% identical duplicate records across all business attributes.',
   passedCount: totalRows - exactDupRows.length,
   failedCount: exactDupRows.length,
   totalCount: totalRows,
   passRate: Number((((totalRows - exactDupRows.length) / totalRows) * 100).toFixed(1)),
   status: exactDupRows.length === 0 ? 'Passed' : 'Failed',
   remediation: 'Enable Deduplication node (drop exact duplicates) in the pipeline.',
   affectedRows: exactDupRows
  },
  {
   id: 'chk-key-dup',
   name: 'Business Key Uniqueness (order_id)',
   dimension: 'Uniqueness',
   description: 'Ensures primary transaction identifier (order_id) has zero collisions.',
   passedCount: totalRows - keyDupRows.length,
   failedCount: keyDupRows.length,
   totalCount: totalRows,
   passRate: uniquenessScore,
   status: keyDupRows.length === 0 ? 'Passed' : 'Failed',
   remediation: 'Configure deduplication by business key `order_id` (keep latest record).',
   affectedRows: keyDupRows
  },
  {
   id: 'chk-date-valid',
   name: 'ISO-8601 Date Format & Calendar Conformance',
   dimension: 'Validity',
   description: 'Validates `order_date` against strict YYYY-MM-DD calendar boundaries.',
   passedCount: totalRows - invalidDateRows.length,
   failedCount: invalidDateRows.length,
   totalCount: totalRows,
   passRate: Number((((totalRows - invalidDateRows.length) / totalRows) * 100).toFixed(1)),
   status: invalidDateRows.length === 0 ? 'Passed' : 'Failed',
   remediation: 'Filter or quarantine unparseable dates (`2026-13-45`, `INVALID_DATE`) during Validation stage.',
   affectedRows: invalidDateRows
  },
  {
   id: 'chk-numeric-valid',
   name: 'Positive Numeric Domain (quantity & unit_price > 0)',
   dimension: 'Validity',
   description: 'Ensures quantity and unit_price are valid numbers strictly greater than zero.',
   passedCount: totalRows - invalidNumericRows.length,
   failedCount: invalidNumericRows.length,
   totalCount: totalRows,
   passRate: Number((((totalRows - invalidNumericRows.length) / totalRows) * 100).toFixed(1)),
   status: invalidNumericRows.length === 0 ? 'Passed' : 'Failed',
   remediation: 'Convert negative quantities/prices via absolute value normalization and impute null unit prices with median.',
   affectedRows: invalidNumericRows
  },
  {
   id: 'chk-category-consistency',
   name: 'Category Taxonomy & Whitespace Standardization',
   dimension: 'Consistency',
   description: 'Checks that category values follow Title Case taxonomy with no leading/trailing spaces or underscores.',
   passedCount: totalRows - inconsistentCatRows.length,
   failedCount: inconsistentCatRows.length,
   totalCount: totalRows,
   passRate: consistencyScore,
   status: inconsistentCatRows.length === 0 ? 'Passed' : 'Warning',
   remediation: 'Enable Whitespace Normalization & Category Label Standardization in the Transform node.',
   affectedRows: inconsistentCatRows
  },
  {
   id: 'chk-schema-conformance',
   name: 'Mandatory Target Schema Conformance',
   dimension: 'Schema Conformance',
   description: `Verifies presence of all ${requiredCols.length} required columns in dataset schema.`,
   passedCount: requiredCols.length - missingReqCols.length,
   failedCount: missingReqCols.length,
   totalCount: requiredCols.length,
   passRate: Number((((requiredCols.length - missingReqCols.length) / requiredCols.length) * 100).toFixed(1)),
   status: missingReqCols.length === 0 ? 'Passed' : 'Failed',
   remediation: missingReqCols.length === 0 ? 'All required columns verified.' : `Map missing columns: ${missingReqCols.join(', ')}`,
   affectedRows: []
  }
 ];

 return {
  overallScore,
  completenessScore,
  uniquenessScore,
  validityScore,
  consistencyScore,
  totalRows,
  validRows,
  invalidRows: invalidRowsCount,
  duplicateRows: keyDupRows.length,
  exactDuplicateRows: exactDupRows.length,
  inconsistentRows: inconsistentCatRows.length,
  nullCountsByColumn,
  checks
 };
}

function executeTransformationPipeline(
 rawRows: DataRow[],
 config: TransformationRuleConfig
): {
 curatedRows: DataRow[];
 quarantinedRows: DataRow[];
 steps: TransformationStepResult[];
 aggregatedSummary: { groupKey: string; count: number; sumRevenue: number; avgRevenue: number; minRevenue: number; maxRevenue: number }[];
 actualComputeMs: number;
} {
 const t0 = performance.now();
 let working: DataRow[] = rawRows.map((r, idx) => ({ ...r, _rowId: idx + 1 }));
 const quarantinedRows: DataRow[] = [];
 const steps: TransformationStepResult[] = [];

 // STEP 1: DEDUPLICATION
 const s1Start = performance.now();
 const s1In = working.length;
 const s1Before = working.slice(0, 5).map(r => ({ ...r }));
 let s1Removed = 0;

 if (config.deduplicateExact) {
  const seenExact = new Set<string>();
  working = working.filter(r => {
   const copy = { ...r };
   delete copy._rowId;
   const sig = JSON.stringify(copy);
   if (seenExact.has(sig)) {
    s1Removed++;
    quarantinedRows.push({ ...r, _quarantineReason: 'Exact Duplicate Record' });
    return false;
   }
   seenExact.add(sig);
   return true;
  });
 }

 if (config.deduplicateBusinessKey && config.deduplicateBusinessKey !== 'none') {
  const keyCol = config.deduplicateBusinessKey;
  const latestByKey = new Map<string, DataRow>();
  const orderKeys: string[] = [];
  working.forEach(r => {
   const k = String(r[keyCol] ?? '');
   if (!latestByKey.has(k)) {
    orderKeys.push(k);
   } else {
    s1Removed++;
    const prev = latestByKey.get(k)!;
    quarantinedRows.push({ ...prev, _quarantineReason: `Duplicate Business Key (${keyCol})` });
   }
   latestByKey.set(k, r);
  });
  working = orderKeys.map(k => latestByKey.get(k)!);
 }

 steps.push({
  id: 'step-dedup',
  name: 'Deduplicate Exact & Business Key Records',
  type: 'DEDUPLICATE',
  inputColumns: ['order_id', 'order_date', 'product_name', 'category'],
  configSummary: `Exact=${config.deduplicateExact}, Key=${config.deduplicateBusinessKey || 'order_id'} (keep latest)`,
  status: 'Succeeded',
  inputRows: s1In,
  outputRows: working.length,
  removedRows: s1In - working.length,
  modifiedRows: 0,
  durationMs: Math.max(1, Math.round(performance.now() - s1Start)),
  sampleBefore: s1Before,
  sampleAfter: working.slice(0, 5).map(r => ({ ...r }))
 });

 // STEP 2: WHITESPACE & CATEGORY STANDARDIZATION
 const s2Start = performance.now();
 const s2In = working.length;
 const s2Before = working.slice(0, 5).map(r => ({ ...r }));
 let s2Modified = 0;

 working = working.map(r => {
  const next = { ...r };
  let changed = false;
  if (config.normalizeWhitespace) {
   Object.keys(next).forEach(k => {
    if (typeof next[k] === 'string') {
     const trimmed = next[k].trim().replace(/\s+/g, ' ');
     if (trimmed !== next[k]) {
      next[k] = trimmed;
      changed = true;
     }
    }
   });
  }
  if (config.standardizeCategories && next.category !== undefined) {
   const norm = normalizeCategoryLabel(next.category);
   if (norm !== next.category) {
    next.category = norm;
    changed = true;
   }
  }
  if (changed) s2Modified++;
  return next;
 });

 steps.push({
  id: 'step-clean',
  name: 'Normalize Whitespace & Standardize Category Taxonomy',
  type: 'CLEAN',
  inputColumns: ['category', 'product_name', 'region', 'customer_segment'],
  configSummary: `Trim whitespace=${config.normalizeWhitespace}, TitleCase taxonomy=${config.standardizeCategories}`,
  status: 'Succeeded',
  inputRows: s2In,
  outputRows: working.length,
  removedRows: 0,
  modifiedRows: s2Modified,
  durationMs: Math.max(1, Math.round(performance.now() - s2Start)),
  sampleBefore: s2Before,
  sampleAfter: working.slice(0, 5).map(r => ({ ...r }))
 });

 // STEP 3: MISSING VALUE IMPUTATION, NUMERIC CONVERSION & DATE VALIDATION
 const s3Start = performance.now();
 const s3In = working.length;
 const s3Before = working.slice(0, 5).map(r => ({ ...r }));
 let s3Modified = 0;

 // Compute median positive unit_price for imputation
 const validPrices = working
  .map(r => Number(r.unit_price))
  .filter(n => !isNaN(n) && n > 0)
  .sort((a, b) => a - b);
 const medianPrice = validPrices.length > 0 ? validPrices[Math.floor(validPrices.length / 2)] : 399.0;

 const validatedRows: DataRow[] = [];
 working.forEach(r => {
  const next = { ...r };
  let mod = false;

  // Date validation
  const dateValid = isValidISODate(String(next.order_date || ''));
  if (!dateValid) {
   if (config.dropInvalidDates) {
    quarantinedRows.push({ ...next, _quarantineReason: `Invalid ISO Date (${next.order_date})` });
    return;
   } else {
    next.order_date = '2026-09-01';
    mod = true;
   }
  }

  // Null handling
  const hasMissing =
   next.customer_segment === null || next.customer_segment === undefined || String(next.customer_segment).trim() === '' ||
   next.region === null || next.region === undefined || String(next.region).trim() === '' ||
   next.unit_price === null || next.unit_price === undefined || String(next.unit_price).trim() === '';

  if (hasMissing) {
   if (config.imputeMissingValues) {
    if (!next.customer_segment || String(next.customer_segment).trim() === '') {
     next.customer_segment = 'Enterprise';
     mod = true;
    }
    if (!next.region || String(next.region).trim() === '') {
     next.region = 'North America';
     mod = true;
    }
    if (next.unit_price === null || next.unit_price === undefined || String(next.unit_price).trim() === '' || isNaN(Number(next.unit_price))) {
     next.unit_price = medianPrice;
     mod = true;
    }
   } else {
    quarantinedRows.push({ ...next, _quarantineReason: 'Missing Required Field (Null Drop Policy)' });
    return;
   }
  }

  // Numeric conversion & sign normalization
  let q = Number(next.quantity);
  let p = Number(next.unit_price);
  if (isNaN(q) || q === 0) q = 1;
  if (isNaN(p) || p === 0) p = medianPrice;

  if (q < 0 || p < 0) {
   if (config.fixNegativeNumbers) {
    q = Math.abs(q);
    p = Math.abs(p);
    mod = true;
   } else {
    quarantinedRows.push({ ...next, _quarantineReason: `Negative Numeric Value (qty=${q}, price=${p})` });
    return;
   }
  }

  next.quantity = q;
  next.unit_price = Number(p.toFixed(2));
  if (mod) s3Modified++;
  validatedRows.push(next);
 });

 working = validatedRows;

 steps.push({
  id: 'step-validate-convert',
  name: 'Validate Dates, Impute Missing Fields & Convert Numeric Types',
  type: 'TYPE_CONVERSION',
  inputColumns: ['order_date', 'quantity', 'unit_price', 'region', 'customer_segment'],
  configSummary: `DropInvalidDates=${config.dropInvalidDates}, ImputeNulls=${config.imputeMissingValues} (median=$${medianPrice}), FixNegative=${config.fixNegativeNumbers}`,
  status: 'Succeeded',
  inputRows: s3In,
  outputRows: working.length,
  removedRows: s3In - working.length,
  modifiedRows: s3Modified,
  durationMs: Math.max(1, Math.round(performance.now() - s3Start)),
  sampleBefore: s3Before,
  sampleAfter: working.slice(0, 5).map(r => ({ ...r }))
 });

 // STEP 4: FEATURE ENGINEERING
 const s4Start = performance.now();
 const s4In = working.length;
 const s4Before = working.slice(0, 5).map(r => ({ ...r }));

 working = working.map(r => {
  const next = { ...r };
  const qty = Number(next.quantity) || 0;
  const price = Number(next.unit_price) || 0;
  const rev = Number((qty * price).toFixed(2));
  if (config.deriveRevenue) {
   next.revenue = rev;
  }
  if (config.deriveDateParts && typeof next.order_date === 'string') {
   const parts = next.order_date.split('-');
   if (parts.length === 3) {
    next.order_year = parseInt(parts[0], 10);
    next.order_month = `${parts[0]}-${parts[1]}`;
    next.order_day = parseInt(parts[2], 10);
   }
  }
  if (config.deriveDealTier) {
   next.deal_size_tier =
    rev >= 10000
     ? 'Strategic (>= $10k)'
     : rev >= 3000
     ? 'Mid-Tier ($3k-$10k)'
     : 'Standard (< $3k)';
  }
  return next;
 });

 steps.push({
  id: 'step-feature-eng',
  name: 'Derive Revenue, Temporal Hierarchy & Deal Size Tier',
  type: 'FEATURE_ENGINEERING',
  inputColumns: ['quantity', 'unit_price', 'order_date'],
  configSummary: `Derive revenue=qty*price, Extract year/month/day, Compute deal_size_tier`,
  status: 'Succeeded',
  inputRows: s4In,
  outputRows: working.length,
  removedRows: 0,
  modifiedRows: working.length,
  durationMs: Math.max(1, Math.round(performance.now() - s4Start)),
  sampleBefore: s4Before,
  sampleAfter: working.slice(0, 5).map(r => ({ ...r }))
 });

 // STEP 5: CONDITIONAL FILTERING (IF ENABLED)
 if (config.filterEnabled && config.filterColumn) {
  const s5Start = performance.now();
  const s5In = working.length;
  const s5Before = working.slice(0, 5).map(r => ({ ...r }));
  const col = config.filterColumn;
  const op = config.filterOperator;
  const val = config.filterValue;

  working = working.filter(r => {
   const cell = r[col];
   if (op === 'not_null') {
    return cell !== null && cell !== undefined && String(cell).trim() !== '';
   }
   if (op === 'gt') {
    return Number(cell) > Number(val);
   }
   if (op === 'lt') {
    return Number(cell) < Number(val);
   }
   if (op === 'equals') {
    return String(cell ?? '').toLowerCase() === String(val).trim().toLowerCase();
   }
   if (op === 'contains') {
    return String(cell ?? '').toLowerCase().includes(String(val).trim().toLowerCase());
   }
   if (op === 'date_range') {
    const start = val || '2026-08-01';
    const end = config.filterValueEnd || '2026-12-31';
    const dStr = String(cell ?? '');
    return dStr >= start && dStr <= end;
   }
   return true;
  });

  steps.push({
   id: 'step-filter',
   name: `Filter Records (${col} ${op} ${val})`,
   type: 'FILTER',
   inputColumns: [col],
   configSummary: `${col} ${op} "${val}"`,
   status: 'Succeeded',
   inputRows: s5In,
   outputRows: working.length,
   removedRows: s5In - working.length,
   modifiedRows: 0,
   durationMs: Math.max(1, Math.round(performance.now() - s5Start)),
   sampleBefore: s5Before,
   sampleAfter: working.slice(0, 5).map(r => ({ ...r }))
  });
 }

 // STEP 6: AGGREGATION SUMMARY GENERATION
 const s6Start = performance.now();
 const groupCol = config.aggregateGroupBy || 'category';
 const groups = new Map<string, number[]>();
 working.forEach(r => {
  const key = String(r[groupCol] ?? 'Unknown');
  const rev = Number(r.revenue ?? (Number(r.quantity || 0) * Number(r.unit_price || 0)));
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key)!.push(rev);
 });

 const aggregatedSummary = Array.from(groups.entries()).map(([groupKey, vals]) => {
  const sumRevenue = Number(vals.reduce((a, b) => a + b, 0).toFixed(2));
  const count = vals.length;
  const avgRevenue = count > 0 ? Number((sumRevenue / count).toFixed(2)) : 0;
  const minRevenue = count > 0 ? Number(Math.min(...vals).toFixed(2)) : 0;
  const maxRevenue = count > 0 ? Number(Math.max(...vals).toFixed(2)) : 0;
  return { groupKey, count, sumRevenue, avgRevenue, minRevenue, maxRevenue };
 }).sort((a, b) => b.sumRevenue - a.sumRevenue);

 steps.push({
  id: 'step-aggregate',
  name: `Group & Aggregate Metrics by ${groupCol}`,
  type: 'AGGREGATE',
  inputColumns: [groupCol, 'revenue', 'quantity'],
  configSummary: `GROUP BY ${groupCol} -> Calculate COUNT, SUM, MEAN, MIN, MAX (Primary: ${config.aggregateMetric.toUpperCase()})`,
  status: 'Succeeded',
  inputRows: working.length,
  outputRows: aggregatedSummary.length,
  removedRows: 0,
  modifiedRows: aggregatedSummary.length,
  durationMs: Math.max(1, Math.round(performance.now() - s6Start)),
  sampleBefore: working.slice(0, 3).map(r => ({ ...r })),
  sampleAfter: aggregatedSummary.slice(0, 5).map(a => ({
   order_id: `AGG-${a.groupKey.slice(0, 6).toUpperCase()}`,
   order_date: '2026-10-21',
   product_name: `${a.count} Orders`,
   category: a.groupKey,
   quantity: a.count,
   unit_price: a.avgRevenue,
   region: 'All Regions',
   customer_segment: 'Aggregated',
   revenue: a.sumRevenue
  }))
 });

 const actualComputeMs = Math.max(2, Math.round(performance.now() - t0));
 return { curatedRows: working, quarantinedRows, steps, aggregatedSummary, actualComputeMs };
}

function parseUploadedCSV(csvText: string): DataRow[] {
 const lines = csvText
  .replace(/\r\n/g, '\n')
  .replace(/\r/g, '\n')
  .split('\n')
  .filter(l => l.trim().length > 0);
 if (lines.length < 2) return [];

 const splitCSVLine = (line: string): string[] => {
  const res: string[] = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
   const ch = line[i];
   if (ch === '"') {
    inQuotes = !inQuotes;
   } else if (ch === ',' && !inQuotes) {
    res.push(cur);
    cur = '';
   } else {
    cur += ch;
   }
  }
  res.push(cur);
  return res;
 };

 const headers = splitCSVLine(lines[0]).map(h => h.trim());
 const rows: DataRow[] = [];

 for (let i = 1; i < lines.length; i++) {
  const vals = splitCSVLine(lines[i]);
  const obj: any = {};
  headers.forEach((h, idx) => {
   const rawVal = vals[idx] !== undefined ? vals[idx] : '';
   if (rawVal.trim() === '') {
    obj[h] = null;
   } else if ((h === 'quantity' || h === 'unit_price' || h === 'revenue') && !isNaN(Number(rawVal))) {
    obj[h] = Number(rawVal);
   } else {
    obj[h] = rawVal;
   }
  });
  rows.push(obj as DataRow);
 }
 return rows;
}

function triggerFileDownload(content: string, filename: string, mimeType: string = 'text/csv;charset=utf-8;') {
 try {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
 } catch (e) {
  // Fallback ignored in restricted iframe
 }
}

function rowsToCSV(rows: DataRow[]): string {
 if (!rows || rows.length === 0) return '';
 const headers = Object.keys(rows[0]).filter(k => !k.startsWith('_'));
 const escapeCell = (val: any) => {
  if (val === null || val === undefined) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
   return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
 };
 const lines = [headers.join(',')];
 rows.forEach(r => {
  lines.push(headers.map(h => escapeCell(r[h])).join(','));
 });
 return lines.join('\n');
}

// ============================================================================
// 4. REUSABLE ANIMATED UI & SIGNATURE PIPELINE COMPONENTS
// ============================================================================

const AnimatedNumber: React.FC<{
 value: number;
 prefix?: string;
 suffix?: string;
 decimals?: number;
 reducedMotion?: boolean;
}> = ({ value, prefix = '', suffix = '', decimals = 0, reducedMotion = false }) => {
 const [displayVal, setDisplayVal] = useState<number>(reducedMotion ? value : Math.floor(value * 0.65));

 useEffect(() => {
  if (reducedMotion) {
   setDisplayVal(value);
   return;
  }
  let startVal = displayVal;
  const diff = value - startVal;
  if (Math.abs(diff) < 0.01) {
   setDisplayVal(value);
   return;
  }
  const duration = 280;
  const startTime = performance.now();
  let rafId: number;

  const tick = (now: number) => {
   const elapsed = now - startTime;
   const progress = Math.min(1, elapsed / duration);
   const eased = 1 - Math.pow(1 - progress, 3);
   setDisplayVal(startVal + diff * eased);
   if (progress < 1) {
    rafId = requestAnimationFrame(tick);
   } else {
    setDisplayVal(value);
   }
  };
  rafId = requestAnimationFrame(tick);
  return () => cancelAnimationFrame(rafId);
 }, [value, reducedMotion]);

 const formatted = useMemo(() => {
  return displayVal.toLocaleString('en-US', {
   minimumFractionDigits: decimals,
   maximumFractionDigits: decimals
  });
 }, [displayVal, decimals]);

 return (
  <span className="tabular-nums tracking-tight">
   {prefix}{formatted}{suffix}
  </span>
 );
};

const StatusBadge: React.FC<{ status: string; size?: 'sm' | 'md' }> = ({ status, size = 'sm' }) => {
 const s = status.toLowerCase();
 let cls = 'bg-slate-100 text-slate-700 border-slate-200 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-700';
 let dot = 'bg-slate-400';

 if (s.includes('succeeded') || s.includes('passed') || s.includes('curated') || s.includes('verified') || s.includes('active') || s.includes('connected')) {
  cls = 'bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-950/60 dark:text-emerald-300 dark:border-emerald-800/70';
  dot = 'bg-emerald-500';
 } else if (s.includes('running') || s.includes('in progress') || s.includes('queued')) {
  cls = 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/60 dark:text-blue-300 dark:border-blue-800/70';
  dot = 'bg-blue-500 animate-pulse';
 } else if (s.includes('failed') || s.includes('error') || s.includes('critical')) {
  cls = 'bg-red-50 text-red-700 border-red-200 dark:bg-red-950/60 dark:text-red-300 dark:border-red-800/70';
  dot = 'bg-red-500';
 } else if (s.includes('warning') || s.includes('simulated') || s.includes('demo') || s.includes('raw')) {
  cls = 'bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-950/60 dark:text-amber-300 dark:border-amber-800/70';
  dot = 'bg-amber-500';
 }

 return (
  <span className={`inline-flex items-center gap-1.5 font-medium border rounded-full ${size === 'sm' ? 'px-2.5 py-0.5 text-xs' : 'px-3 py-1 text-xs'} ${cls}`}>
   <span className={`w-1.5 h-1.5 rounded-full ${dot}`} />
   {status}
  </span>
 );
};

// Signature 7-Stage Pipeline Execution Visualizer
interface SignaturePipelineProps {
 execution: PipelineExecutionRecord | null;
 isExecuting: boolean;
 onRunPipeline: (simulateFailure?: boolean) => void;
 onRetryPipeline: () => void;
 selectedStageIndex: number;
 onSelectStage: (idx: number) => void;
 darkMode: boolean;
}

const SignaturePipelineVisualizer: React.FC<SignaturePipelineProps> = ({
 execution,
 isExecuting,
 onRunPipeline,
 onRetryPipeline,
 selectedStageIndex,
 onSelectStage,
 darkMode
}) => {
 const defaultStages: StageExecutionRecord[] = [
  { stageId: 'stg-1', stageName: 'Data Source', nodeType: 'SOURCE', azureService: 'CSV / JSON Source', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 12, logs: ['Loaded 52 raw source records from ecommerce_orders_2026.csv'] },
  { stageId: 'stg-2', stageName: 'Ingestion', nodeType: 'INGEST', azureService: 'Azure Data Factory', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 28, logs: ['ADF Copy Activity transferred 52 rows to Bronze landing zone'] },
  { stageId: 'stg-3', stageName: 'Raw Storage', nodeType: 'INGEST', azureService: 'ADLS Gen2 (Raw)', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 18, logs: ['Persisted raw snapshot at abfss://raw@azurepulsedatalake/bronze/'] },
  { stageId: 'stg-4', stageName: 'Data Validation', nodeType: 'VALIDATE', azureService: 'Quality Gate Engine', status: 'Succeeded', inputRows: 52, outputRows: 50, durationMs: 34, logs: ['Evaluated 7 schema & quality rules; quarantined 2 unparseable date rows'] },
  { stageId: 'stg-5', stageName: 'Transformation', nodeType: 'TRANSFORM', azureService: 'Azure Databricks', status: 'Succeeded', inputRows: 50, outputRows: 46, durationMs: 49, logs: ['Removed 4 duplicates, normalized 11 category labels, imputed 4 nulls, derived revenue & deal tiers'] },
  { stageId: 'stg-6', stageName: 'Curated Storage', nodeType: 'CURATED_STORAGE', azureService: 'ADLS Gen2 (Gold)', status: 'Succeeded', inputRows: 46, outputRows: 46, durationMs: 21, logs: ['Committed 46 clean records to Delta Lake Gold table (Quality Score: 100.0%)'] },
  { stageId: 'stg-7', stageName: 'Analytics', nodeType: 'ANALYTICS', azureService: 'Azure Synapse SQL', status: 'Succeeded', inputRows: 46, outputRows: 46, durationMs: 16, logs: ['Refreshed Synapse Serverless SQL views & executive KPI aggregations ($319,658.50)'] }
 ];

 const stages = execution?.stages && execution.stages.length === 7 ? execution.stages : defaultStages;
 const activeStage = stages[selectedStageIndex] || stages[0];

 const completedCount = stages.filter(s => s.status === 'Succeeded').length;
 const hasFailed = stages.some(s => s.status === 'Failed');
 const progressPct = Math.round((completedCount / stages.length) * 100);

 return (
  <div className={`rounded-xl border transition-all duration-200 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
   {/* Top Bar */}
   <div className={`px-5 py-4 border-b flex flex-wrap items-center justify-between gap-3 ${darkMode ? 'border-slate-800' : 'border-slate-100'}`}>
    <div className="flex items-center gap-3">
     <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-[#0078D4] to-indigo-600 flex items-center justify-center text-white shadow-sm">
      <Workflow className="w-5 h-5" />
     </div>
     <div>
      <div className="flex items-center gap-2.5">
       <h3 className="font-semibold text-sm md:text-base tracking-tight">
        Live Data Engineering Pipeline Lifecycle
       </h3>
       <StatusBadge status={execution?.status || 'Succeeded'} />
       {execution && (
        <span className="text-xs font-mono text-slate-400 tabular-nums">
         #{execution.executionId}
        </span>
       )}
      </div>
      <p className={`text-xs mt-0.5 ${darkMode ? 'text-slate-400' : 'text-slate-500'}`}>
       Click any pipeline stage node below to inspect input/output record counts, duration, and execution logs.
      </p>
     </div>
    </div>

    <div className="flex items-center gap-2">
     <div className={`hidden sm:flex items-center gap-3 px-3 py-1.5 rounded-lg border text-xs tabular-nums ${darkMode ? 'bg-slate-800/70 border-slate-700 text-slate-300' : 'bg-slate-50 border-slate-200 text-slate-600'}`}>
      <span>Compute: <strong>{execution?.actualComputeMs ?? 4} ms</strong></span>
      <span className="text-slate-300 dark:text-slate-600">|</span>
      <span>Total Duration: <strong>{execution?.totalDurationMs ?? 178} ms</strong></span>
      <span className="text-slate-300 dark:text-slate-600">|</span>
      <span>Rows: <strong>{execution?.inputRowCount ?? 52} → {execution?.outputRowCount ?? 46}</strong></span>
     </div>

     {hasFailed && !isExecuting && (
      <button
       type="button"
       onClick={onRetryPipeline}
       className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-amber-500 hover:bg-amber-600 text-white shadow-sm transition-all cursor-pointer"
      >
       <RotateCcw className="w-3.5 h-3.5" />
       Retry Failed Pipeline
      </button>
     )}

     <button
      type="button"
      disabled={isExecuting}
      onClick={() => onRunPipeline(false)}
      className={`inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-medium text-white shadow-sm transition-all cursor-pointer ${
       isExecuting
        ? 'bg-blue-400 cursor-not-allowed'
        : 'bg-[#0078D4] hover:bg-blue-700 active:scale-[0.99]'
      }`}
     >
      {isExecuting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
      {isExecuting ? 'Executing Pipeline...' : 'Run Pipeline'}
     </button>
    </div>
   </div>

   {/* Progress Bar */}
   <div className="w-full h-1 bg-slate-100 dark:bg-slate-800 overflow-hidden">
    <div
     className={`h-full transition-all duration-300 ${
      hasFailed
       ? 'bg-red-500'
       : isExecuting
       ? 'bg-gradient-to-r from-[#0078D4] via-indigo-500 to-emerald-500'
       : 'bg-emerald-500'
     }`}
     style={{ width: `${isExecuting ? Math.max(15, progressPct) : hasFailed ? 60 : 100}%` }}
    />
   </div>

   {/* 7 Connected Stage Nodes */}
   <div className="p-5 overflow-x-auto">
    <div className="min-w-[820px] grid grid-cols-7 gap-2 relative">
     {stages.map((stg, idx) => {
      const isSelected = selectedStageIndex === idx;
      const isRunning = stg.status === 'Running';
      const isSucceeded = stg.status === 'Succeeded';
      const isFailed = stg.status === 'Failed';

      let borderClass = darkMode ? 'border-slate-800 bg-slate-800/40' : 'border-slate-200 bg-slate-50/70';
      if (isRunning) {
       borderClass = 'border-[#0078D4] bg-blue-50/80 dark:bg-blue-950/50 ring-2 ring-[#0078D4]/30';
      } else if (isFailed) {
       borderClass = 'border-red-500 bg-red-50/80 dark:bg-red-950/50 ring-2 ring-red-500/30';
      } else if (isSucceeded) {
       borderClass = isSelected
        ? 'border-[#0078D4] bg-blue-50/40 dark:bg-blue-950/30 ring-2 ring-[#0078D4]/25'
        : darkMode
        ? 'border-emerald-800/60 bg-slate-800/70 hover:border-slate-700'
        : 'border-emerald-200/90 bg-white hover:border-blue-300';
      }

      return (
       <div key={stg.stageId} className="relative flex items-center">
        <button
         type="button"
         onClick={() => onSelectStage(idx)}
         className={`w-full text-left p-3 rounded-xl border transition-all duration-200 cursor-pointer relative z-10 ${borderClass}`}
        >
         <div className="flex items-center justify-between mb-1.5">
          <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400 tabular-nums">
           0{idx + 1}
          </span>
          {isRunning && <Loader2 className="w-3.5 h-3.5 text-[#0078D4] animate-spin" />}
          {isSucceeded && <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />}
          {isFailed && <XCircle className="w-3.5 h-3.5 text-red-500" />}
          {stg.status === 'Pending' && <Clock className="w-3.5 h-3.5 text-slate-400" />}
          {stg.status === 'Skipped' && <AlertCircle className="w-3.5 h-3.5 text-slate-400" />}
         </div>

         <div className="font-semibold text-xs truncate">{stg.stageName}</div>
         <div className={`text-[11px] truncate mt-0.5 ${darkMode ? 'text-slate-400' : 'text-slate-500'}`}>
          {stg.azureService}
         </div>

         <div className="mt-2.5 pt-2 border-t border-slate-200/60 dark:border-slate-700/60 flex items-center justify-between text-[11px] tabular-nums">
          <span className={darkMode ? 'text-slate-300' : 'text-slate-600'}>
           {stg.outputRows} rows
          </span>
          <span className="text-slate-400 font-mono">
           {stg.durationMs}ms
          </span>
         </div>
        </button>

        {/* Animated Connector Arrow */}
        {idx < stages.length - 1 && (
         <div className="w-3 flex items-center justify-center -mr-2 z-0">
          <div
           className={`h-0.5 w-full transition-colors duration-300 ${
            isRunning
             ? 'bg-[#0078D4] animate-pulse'
             : isSucceeded
             ? 'bg-emerald-400 dark:bg-emerald-600'
             : 'bg-slate-200 dark:bg-slate-700'
           }`}
          />
         </div>
        )}
       </div>
      );
     })}
    </div>

    {/* Stage Inspector Drawer */}
    {activeStage && (
     <div className={`mt-4 p-3.5 rounded-xl border text-xs transition-all ${darkMode ? 'bg-slate-950/70 border-slate-800' : 'bg-slate-50/90 border-slate-200/80'}`}>
      <div className="flex flex-wrap items-center justify-between gap-2 mb-2">
       <div className="flex items-center gap-2">
        <span className="font-semibold text-[#0078D4] dark:text-blue-400">
         Stage 0{selectedStageIndex + 1}: {activeStage.stageName}
        </span>
        <span className="text-slate-400">•</span>
        <span className={darkMode ? 'text-slate-300' : 'text-slate-600'}>{activeStage.azureService}</span>
        <StatusBadge status={activeStage.status} />
       </div>
       <div className="flex items-center gap-4 tabular-nums text-slate-500 dark:text-slate-400">
        <span>Input: <strong>{activeStage.inputRows} rows</strong></span>
        <span>Output: <strong>{activeStage.outputRows} rows</strong></span>
        <span>Duration: <strong>{activeStage.durationMs} ms</strong></span>
       </div>
      </div>
      {activeStage.errorMessage && (
       <div className="mb-2 p-2 rounded-lg bg-red-500/10 border border-red-500/30 text-red-600 dark:text-red-300 flex items-center justify-between">
        <span><strong>Stage Error:</strong> {activeStage.errorMessage}</span>
        <button
         type="button"
         onClick={onRetryPipeline}
         className="px-2.5 py-1 rounded bg-red-600 text-white text-[11px] font-medium hover:bg-red-700 cursor-pointer"
        >
         Fix & Retry Stage
        </button>
       </div>
      )}
      <div className="font-mono text-[11px] text-slate-600 dark:text-slate-400 space-y-1">
       {activeStage.logs.map((logLine, i) => (
        <div key={i} className="flex items-start gap-2">
         <span className="text-slate-400 select-none">&gt;</span>
         <span>{logLine}</span>
        </div>
       ))}
      </div>
     </div>
    )}
   </div>
  </div>
 );
};

// ============================================================================
// 5. OVERVIEW DASHBOARD, ACTIVITY FEED, DATA SOURCES & DATA INGESTION VIEWS
// ============================================================================

interface OverviewDashboardProps {
 datasets: DatasetEntity[];
 activeDatasetId: string;
 onSelectDataset: (id: string) => void;
 dateRange: string;
 onSelectDateRange: (range: string) => void;
 executions: PipelineExecutionRecord[];
 isExecuting: boolean;
 onRunPipeline: (simulateFailure?: boolean) => void;
 onRetryPipeline: () => void;
 alerts: PlatformAlert[];
 onAcknowledgeAlert: (id: string) => void;
 onNavigate: (page: NavPageId) => void;
 reducedMotion: boolean;
 darkMode: boolean;
}

const OverviewDashboardView: React.FC<OverviewDashboardProps> = ({
 datasets,
 activeDatasetId,
 onSelectDataset,
 dateRange,
 onSelectDateRange,
 executions,
 isExecuting,
 onRunPipeline,
 onRetryPipeline,
 alerts,
 onAcknowledgeAlert,
 onNavigate,
 reducedMotion,
 darkMode
}) => {
 const [selectedStageIdx, setSelectedStageIdx] = useState<number>(4);
 const [activeKpiTooltip, setActiveKpiTooltip] = useState<string | null>(null);
 const [qualityLayerToggle, setQualityLayerToggle] = useState<'raw' | 'curated'>('raw');

 const activeDataset = useMemo(
  () => datasets.find(d => d.id === activeDatasetId) || datasets[0],
  [datasets, activeDatasetId]
 );

 // Filter curated rows by selected date range
 const filteredCuratedRows = useMemo(() => {
  const rows = activeDataset?.curatedRows || [];
  if (dateRange === 'ALL') return rows;
  return rows.filter(r => String(r.order_date || '').startsWith(dateRange));
 }, [activeDataset, dateRange]);

 const filteredRawRows = useMemo(() => {
  const rows = activeDataset?.rawRows || [];
  if (dateRange === 'ALL') return rows;
  return rows.filter(r => String(r.order_date || '').startsWith(dateRange));
 }, [activeDataset, dateRange]);

 // Derived KPI values from real application state
 const totalRecordsProcessed = useMemo(
  () => datasets.reduce((acc, d) => acc + d.curatedRows.length, 0),
  [datasets]
 );
 const successfulRuns = useMemo(
  () => executions.filter(e => e.status === 'Succeeded').length,
  [executions]
 );
 const failedRuns = useMemo(
  () => executions.filter(e => e.status === 'Failed').length,
  [executions]
 );
 const avgProcessingTime = useMemo(() => {
  if (executions.length === 0) return 0;
  const sum = executions.reduce((acc, e) => acc + e.totalDurationMs, 0);
  return Math.round(sum / executions.length);
 }, [executions]);

 const latestExecution = executions[0] || null;

 // Business Analytics aggregations from filteredCuratedRows
 const totalRevenue = useMemo(
  () => filteredCuratedRows.reduce((acc, r) => acc + Number(r.revenue ?? Number(r.quantity || 0) * Number(r.unit_price || 0)), 0),
  [filteredCuratedRows]
 );
 const orderCount = filteredCuratedRows.length;
 const avgOrderValue = orderCount > 0 ? totalRevenue / orderCount : 0;

 // A. Data volume & Revenue over time
 const timeSeriesData = useMemo(() => {
  const map = new Map<string, { period: string; rawCount: number; curatedCount: number; revenue: number; orders: number }>();
  filteredRawRows.forEach(r => {
   const d = String(r.order_date || '');
   const key = isValidISODate(d) ? d.slice(0, 7) : 'Invalid Date';
   if (key === 'Invalid Date') return;
   if (!map.has(key)) map.set(key, { period: key, rawCount: 0, curatedCount: 0, revenue: 0, orders: 0 });
   map.get(key)!.rawCount += 1;
  });
  filteredCuratedRows.forEach(r => {
   const d = String(r.order_date || '');
   const key = d.slice(0, 7) || '2026-08';
   if (!map.has(key)) map.set(key, { period: key, rawCount: 0, curatedCount: 0, revenue: 0, orders: 0 });
   const item = map.get(key)!;
   item.curatedCount += 1;
   item.orders += 1;
   item.revenue = Number((item.revenue + Number(r.revenue || 0)).toFixed(2));
  });
  return Array.from(map.values()).sort((a, b) => a.period.localeCompare(b.period));
 }, [filteredRawRows, filteredCuratedRows]);

 // D1. Revenue by Region
 const regionData = useMemo(() => {
  const map = new Map<string, { region: string; revenue: number; orders: number }>();
  filteredCuratedRows.forEach(r => {
   const reg = String(r.region || 'North America');
   if (!map.has(reg)) map.set(reg, { region: reg, revenue: 0, orders: 0 });
   const item = map.get(reg)!;
   item.revenue = Number((item.revenue + Number(r.revenue || 0)).toFixed(2));
   item.orders += 1;
  });
  return Array.from(map.values()).sort((a, b) => b.revenue - a.revenue);
 }, [filteredCuratedRows]);

 // D2. Sales by Category
 const categoryData = useMemo(() => {
  const map = new Map<string, { category: string; revenue: number; quantity: number }>();
  filteredCuratedRows.forEach(r => {
   const cat = String(r.category || 'Electronics');
   if (!map.has(cat)) map.set(cat, { category: cat, revenue: 0, quantity: 0 });
   const item = map.get(cat)!;
   item.revenue = Number((item.revenue + Number(r.revenue || 0)).toFixed(2));
   item.quantity += Number(r.quantity || 0);
  });
  return Array.from(map.values()).sort((a, b) => b.revenue - a.revenue);
 }, [filteredCuratedRows]);

 // D3. Customer Segment Distribution
 const segmentData = useMemo(() => {
  const map = new Map<string, { name: string; value: number; orders: number }>();
  filteredCuratedRows.forEach(r => {
   const seg = String(r.customer_segment || 'Enterprise');
   if (!map.has(seg)) map.set(seg, { name: seg, value: 0, orders: 0 });
   const item = map.get(seg)!;
   item.value = Number((item.value + Number(r.revenue || 0)).toFixed(2));
   item.orders += 1;
  });
  return Array.from(map.values()).sort((a, b) => b.value - a.value);
 }, [filteredCuratedRows]);

 // C. Data Quality Overview Breakdown
 const activeQuality = qualityLayerToggle === 'raw' ? activeDataset.rawQuality : activeDataset.curatedQuality;
 const qualityBreakdownData = useMemo(() => {
  const missingCount = Object.values(activeQuality.nullCountsByColumn).reduce((a, b) => a + b, 0);
  return [
   { name: 'Valid Clean Records', value: activeQuality.validRows, color: '#10B981' },
   { name: 'Inconsistent Taxonomy', value: activeQuality.inconsistentRows, color: '#6366F1' },
   { name: 'Invalid Date/Numeric', value: activeQuality.invalidRows, color: '#EF4444' },
   { name: 'Duplicate Records', value: activeQuality.duplicateRows, color: '#F59E0B' },
   { name: 'Missing Field Cells', value: missingCount, color: '#0078D4' }
  ];
 }, [activeQuality]);

 // B. Pipeline execution history chart data
 const executionChartData = useMemo(() => {
  return executions
   .slice(0, 7)
   .reverse()
   .map(ex => ({
    id: `#${ex.executionId.slice(-4)}`,
    durationMs: ex.totalDurationMs,
    inputRows: ex.inputRowCount,
    outputRows: ex.outputRowCount,
    status: ex.status
   }));
 }, [executions]);

 const kpiCards = [
  {
   id: 'kpi-records',
   label: 'Total Records Processed',
   value: totalRecordsProcessed,
   suffix: '',
   decimals: 0,
   trend: `+${activeDataset.curatedRows.length} in active gold table`,
   trendPositive: true,
   icon: Database,
   accent: 'from-[#0078D4] to-blue-600',
   tooltip: 'Sum of validated, deduplicated records persisted across all curated ADLS Gen2 Gold datasets.',
   targetPage: 'explorer' as NavPageId
  },
  {
   id: 'kpi-success',
   label: 'Successful Pipeline Runs',
   value: successfulRuns,
   suffix: '',
   decimals: 0,
   trend: `${Math.round((successfulRuns / Math.max(1, executions.length)) * 100)}% success rate`,
   trendPositive: true,
   icon: CheckCircle2,
   accent: 'from-emerald-500 to-teal-600',
   tooltip: 'Count of end-to-end pipeline executions completing all 7 stages without error.',
   targetPage: 'monitor' as NavPageId
  },
  {
   id: 'kpi-quality',
   label: 'Data Quality Score',
   value: activeDataset.curatedQuality.overallScore,
   suffix: '%',
   decimals: 1,
   trend: `Up from ${activeDataset.rawQuality.overallScore}% raw baseline`,
   trendPositive: true,
   icon: ShieldCheck,
   accent: 'from-indigo-500 to-blue-600',
   tooltip: 'Weighted quality index: 30% Completeness + 25% Validity + 25% Uniqueness + 20% Consistency.',
   targetPage: 'quality' as NavPageId
  },
  {
   id: 'kpi-failed',
   label: 'Failed Pipeline Runs',
   value: failedRuns,
   suffix: '',
   decimals: 0,
   trend: failedRuns > 0 ? `${failedRuns} eligible for retry` : 'Zero active failures',
   trendPositive: failedRuns === 0,
   icon: AlertTriangle,
   accent: 'from-red-500 to-rose-600',
   tooltip: 'Executions halted due to strict schema mismatch or validation threshold violations.',
   targetPage: 'monitor' as NavPageId
  },
  {
   id: 'kpi-duration',
   label: 'Average Processing Time',
   value: avgProcessingTime,
   suffix: ' ms',
   decimals: 0,
   trend: `${latestExecution?.actualComputeMs ?? 4}ms pure engine compute`,
   trendPositive: true,
   icon: Clock,
   accent: 'from-amber-500 to-orange-500',
   tooltip: 'Mean pipeline lifecycle execution time across all recorded runs.',
   targetPage: 'monitor' as NavPageId
  },
  {
   id: 'kpi-datasets',
   label: 'Active Datasets',
   value: datasets.length,
   suffix: '',
   decimals: 0,
   trend: `${datasets.reduce((a, d) => a + d.rawRows.length, 0)} raw records ingested`,
   trendPositive: true,
   icon: FolderKanban,
   accent: 'from-blue-600 to-indigo-700',
   tooltip: 'Registered datasets with separate Raw (Bronze) and Curated (Gold) storage layers.',
   targetPage: 'sources' as NavPageId
  }
 ];

 const activeAlerts = alerts.filter(a => !a.acknowledged);
 const CHART_COLORS = ['#0078D4', '#4F46E5', '#10B981', '#F59E0B', '#EC4899'];

 return (
  <div className="space-y-6">
   {/* Hero Banner / Landing Header */}
   <div className={`rounded-2xl border p-6 relative overflow-hidden transition-all ${
    darkMode
     ? 'bg-gradient-to-r from-slate-900 via-[#0F172A] to-indigo-950/60 border-slate-800'
     : 'bg-gradient-to-r from-white via-blue-50/40 to-indigo-50/50 border-slate-200/90 shadow-sm'
   }`}>
    <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4 relative z-10">
     <div>
      <div className="inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs font-medium bg-[#0078D4]/10 text-[#0078D4] dark:bg-blue-500/20 dark:text-blue-300 mb-2">
       <Sparkles className="w-3.5 h-3.5" />
       <span>Wednesday, October 21, 2026 • Microsoft Azure Data Platform</span>
      </div>
      <h1 className="text-2xl sm:text-3xl font-bold tracking-tight">
       Good morning, Analyst
      </h1>
      <p className={`text-sm mt-1 ${darkMode ? 'text-slate-400' : 'text-slate-600'}`}>
       Your data pipelines, quality metrics, and analytics at a glance.
      </p>
     </div>

     {/* Controls: Dataset Selector, Date Range Selector, Upload & Run Actions */}
     <div className="flex flex-wrap items-center gap-2.5">
      <div className="flex items-center gap-1.5">
       <label htmlFor="dash-dataset-select" className="text-xs font-medium text-slate-500 dark:text-slate-400">
        Dataset:
       </label>
       <select
        id="dash-dataset-select"
        value={activeDatasetId}
        onChange={e => onSelectDataset(e.target.value)}
        className={`text-xs font-medium rounded-lg border px-2.5 py-2 outline-none transition-colors ${
         darkMode
          ? 'bg-slate-800 border-slate-700 text-slate-100'
          : 'bg-white border-slate-200 text-slate-800 hover:border-slate-300'
        }`}
       >
        {datasets.map(d => (
         <option key={d.id} value={d.id}>
          {d.name} ({d.curatedRows.length} curated / {d.rawRows.length} raw)
         </option>
        ))}
       </select>
      </div>

      <div className="flex items-center gap-1.5">
       <label htmlFor="dash-date-select" className="text-xs font-medium text-slate-500 dark:text-slate-400">
        Period:
       </label>
       <select
        id="dash-date-select"
        value={dateRange}
        onChange={e => onSelectDateRange(e.target.value)}
        className={`text-xs font-medium rounded-lg border px-2.5 py-2 outline-none transition-colors ${
         darkMode
          ? 'bg-slate-800 border-slate-700 text-slate-100'
          : 'bg-white border-slate-200 text-slate-800 hover:border-slate-300'
        }`}
       >
        <option value="ALL">All Periods (Aug – Oct 2026)</option>
        <option value="2026-08">August 2026</option>
        <option value="2026-09">September 2026</option>
        <option value="2026-10">October 2026</option>
       </select>
      </div>

      <button
       type="button"
       onClick={() => onNavigate('ingestion')}
       className={`inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg text-xs font-medium border transition-all cursor-pointer ${
        darkMode
         ? 'bg-slate-800 border-slate-700 text-slate-100 hover:bg-slate-700'
         : 'bg-white border-slate-200 text-slate-700 hover:bg-slate-50 shadow-sm'
       }`}
      >
       <Upload className="w-3.5 h-3.5 text-[#0078D4]" />
       Upload Dataset
      </button>

      <button
       type="button"
       disabled={isExecuting}
       onClick={() => onRunPipeline(false)}
       className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] hover:bg-blue-700 text-white shadow-sm transition-all cursor-pointer disabled:opacity-60"
      >
       {isExecuting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
       Run Pipeline
      </button>
     </div>
    </div>
   </div>

   {/* 6 Staggered KPI Cards */}
   <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-3.5">
    {kpiCards.map((card, idx) => {
     const IconComponent = card.icon;
     return (
      <div
       key={card.id}
       onClick={() => onNavigate(card.targetPage)}
       onMouseEnter={() => setActiveKpiTooltip(card.id)}
       onMouseLeave={() => setActiveKpiTooltip(null)}
       style={{ animationDelay: `${idx * 45}ms` }}
       className={`group relative rounded-xl border p-4 transition-all duration-200 cursor-pointer hover:-translate-y-0.5 ${
        darkMode
         ? 'bg-slate-900/90 border-slate-800 hover:border-slate-700'
         : 'bg-white border-slate-200/90 hover:border-blue-300 shadow-sm hover:shadow-md'
       }`}
      >
       <div className="flex items-center justify-between mb-2.5">
        <span className={`text-xs font-medium ${darkMode ? 'text-slate-400' : 'text-slate-500'}`}>
         {card.label}
        </span>
        <div className={`w-7 h-7 rounded-lg bg-gradient-to-br ${card.accent} flex items-center justify-center text-white shadow-sm`}>
         <IconComponent className="w-3.5 h-3.5" />
        </div>
       </div>

       <div className="text-2xl font-bold tracking-tight mb-1.5">
        <AnimatedNumber
         value={card.value}
         suffix={card.suffix}
         decimals={card.decimals}
         reducedMotion={reducedMotion}
        />
       </div>

       <div className="flex items-center justify-between text-[11px]">
        <span className={`inline-flex items-center gap-1 font-medium ${card.trendPositive ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-600 dark:text-amber-400'}`}>
         {card.trendPositive ? <ArrowUpRight className="w-3 h-3" /> : <AlertCircle className="w-3 h-3" />}
         {card.trend}
        </span>
        <ArrowRight className="w-3 h-3 opacity-0 group-hover:opacity-100 text-[#0078D4] transition-opacity" />
       </div>

       {activeKpiTooltip === card.id && (
        <div className="absolute z-30 left-2 right-2 -bottom-2 translate-y-full p-2.5 rounded-lg bg-slate-900 text-white text-[11px] leading-snug shadow-xl border border-slate-700 pointer-events-none">
         {card.tooltip}
        </div>
       )}
      </div>
     );
    })}
   </div>

   {/* Signature Pipeline Status Panel */}
   <SignaturePipelineVisualizer
    execution={latestExecution}
    isExecuting={isExecuting}
    onRunPipeline={onRunPipeline}
    onRetryPipeline={onRetryPipeline}
    selectedStageIndex={selectedStageIdx}
    onSelectStage={setSelectedStageIdx}
    darkMode={darkMode}
   />

   {/* Row 1 Visualizations: A. Data Volume Over Time, B. Pipeline Execution History, C. Data Quality Overview */}
   <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
    {/* A. Data Volume Over Time */}
    <div className={`rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
     <div className="flex items-center justify-between mb-4">
      <div>
       <h3 className="font-semibold text-sm">Data Volume Over Time</h3>
       <p className="text-xs text-slate-500 dark:text-slate-400">
        Raw ingested vs. Curated gold records by month
       </p>
      </div>
      <span className="text-xs font-mono px-2 py-0.5 rounded bg-blue-500/10 text-[#0078D4] dark:text-blue-400 tabular-nums">
       {filteredRawRows.length} raw / {filteredCuratedRows.length} clean
      </span>
     </div>
     <div className="h-56">
      <ResponsiveContainer width="100%" height="100%">
       <BarChart data={timeSeriesData}>
        <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#f1f5f9'} />
        <XAxis dataKey="period" tick={{ fontSize: 11, fill: darkMode ? '#94a3b8' : '#64748b' }} />
        <YAxis tick={{ fontSize: 11, fill: darkMode ? '#94a3b8' : '#64748b' }} />
        <RechartsTooltip
         contentStyle={{
          backgroundColor: darkMode ? '#0f172a' : '#ffffff',
          borderColor: darkMode ? '#334155' : '#e2e8f0',
          borderRadius: '8px',
          fontSize: '12px'
         }}
        />
        <Legend wrapperStyle={{ fontSize: '11px' }} />
        <Bar dataKey="rawCount" name="Raw Ingested" fill="#94A3B8" radius={[4, 4, 0, 0]} />
        <Bar dataKey="curatedCount" name="Curated Gold" fill="#0078D4" radius={[4, 4, 0, 0]} />
       </BarChart>
      </ResponsiveContainer>
     </div>
    </div>

    {/* B. Pipeline Execution History */}
    <div className={`rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
     <div className="flex items-center justify-between mb-4">
      <div>
       <h3 className="font-semibold text-sm">Pipeline Execution History</h3>
       <p className="text-xs text-slate-500 dark:text-slate-400">
        Duration (ms) and output volume across recent runs
       </p>
      </div>
      <button
       type="button"
       onClick={() => onNavigate('monitor')}
       className="text-xs font-medium text-[#0078D4] hover:underline cursor-pointer"
      >
       View Monitor →
      </button>
     </div>
     <div className="h-56">
      <ResponsiveContainer width="100%" height="100%">
       <AreaChart data={executionChartData}>
        <defs>
         <linearGradient id="execGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="5%" stopColor="#4F46E5" stopOpacity={0.35} />
          <stop offset="95%" stopColor="#4F46E5" stopOpacity={0.02} />
         </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#f1f5f9'} />
        <XAxis dataKey="id" tick={{ fontSize: 11, fill: darkMode ? '#94a3b8' : '#64748b' }} />
        <YAxis tick={{ fontSize: 11, fill: darkMode ? '#94a3b8' : '#64748b' }} />
        <RechartsTooltip
         contentStyle={{
          backgroundColor: darkMode ? '#0f172a' : '#ffffff',
          borderColor: darkMode ? '#334155' : '#e2e8f0',
          borderRadius: '8px',
          fontSize: '12px'
         }}
        />
        <Legend wrapperStyle={{ fontSize: '11px' }} />
        <Area type="monotone" dataKey="durationMs" name="Duration (ms)" stroke="#4F46E5" fill="url(#execGrad)" strokeWidth={2} />
        <Area type="monotone" dataKey="outputRows" name="Output Rows" stroke="#10B981" fill="none" strokeWidth={2} />
       </AreaChart>
      </ResponsiveContainer>
     </div>
    </div>

    {/* C. Data Quality Overview */}
    <div className={`rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
     <div className="flex items-center justify-between mb-3">
      <div>
       <h3 className="font-semibold text-sm">Data Quality Overview</h3>
       <p className="text-xs text-slate-500 dark:text-slate-400">
        Record health distribution ({qualityLayerToggle === 'raw' ? 'Raw Layer' : 'Curated Gold'})
       </p>
      </div>
      <div className="inline-flex rounded-lg p-0.5 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700">
       <button
        type="button"
        onClick={() => setQualityLayerToggle('raw')}
        className={`px-2 py-1 rounded-md text-[11px] font-medium transition-colors cursor-pointer ${
         qualityLayerToggle === 'raw'
          ? 'bg-white dark:bg-slate-900 text-[#0078D4] shadow-sm'
          : 'text-slate-500'
        }`}
       >
        Raw ({activeDataset.rawQuality.overallScore}%)
       </button>
       <button
        type="button"
        onClick={() => setQualityLayerToggle('curated')}
        className={`px-2 py-1 rounded-md text-[11px] font-medium transition-colors cursor-pointer ${
         qualityLayerToggle === 'curated'
          ? 'bg-white dark:bg-slate-900 text-emerald-600 shadow-sm'
          : 'text-slate-500'
        }`}
       >
        Curated ({activeDataset.curatedQuality.overallScore}%)
       </button>
      </div>
     </div>

     <div className="grid grid-cols-2 gap-2 items-center">
      <div className="h-48">
       <ResponsiveContainer width="100%" height="100%">
        <PieChart>
         <Pie
          data={qualityBreakdownData}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          innerRadius={42}
          outerRadius={68}
          paddingAngle={2}
         >
          {qualityBreakdownData.map((entry, index) => (
           <Cell key={`cell-${index}`} fill={entry.color} />
          ))}
         </Pie>
         <RechartsTooltip
          contentStyle={{
           backgroundColor: darkMode ? '#0f172a' : '#ffffff',
           borderColor: darkMode ? '#334155' : '#e2e8f0',
           borderRadius: '8px',
           fontSize: '12px'
          }}
         />
        </PieChart>
       </ResponsiveContainer>
      </div>

      <div className="space-y-2 text-xs">
       {qualityBreakdownData.map(item => (
        <div key={item.name} className="flex items-center justify-between">
         <span className="flex items-center gap-1.5 text-slate-600 dark:text-slate-300 truncate">
          <span className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ backgroundColor: item.color }} />
          <span className="truncate">{item.name}</span>
         </span>
         <span className="font-semibold tabular-nums ml-2">{item.value}</span>
        </div>
       ))}
       <button
        type="button"
        onClick={() => onNavigate('quality')}
        className="w-full mt-2 pt-2 border-t border-slate-100 dark:border-slate-800 text-left text-[11px] font-medium text-[#0078D4] hover:underline cursor-pointer"
       >
        Inspect Quality Rules & Affected Rows →
       </button>
      </div>
     </div>
    </div>
   </div>

   {/* D. Business Analytics Section (Calculated dynamically from Curated Dataset) */}
   <div className={`rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
    <div className="flex flex-wrap items-center justify-between gap-3 mb-5">
     <div>
      <div className="flex items-center gap-2">
       <h3 className="font-semibold text-base">Curated Business Analytics</h3>
       <span className="text-xs px-2.5 py-0.5 rounded-full bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 font-medium">
        Live from {activeDataset.name}
       </span>
      </div>
      <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
       Aggregated directly from {filteredCuratedRows.length} validated Gold records.
      </p>
     </div>

     <div className="flex flex-wrap items-center gap-4 text-xs tabular-nums">
      <div className={`px-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800/70 border-slate-700' : 'bg-slate-50 border-slate-200'}`}>
       <span className="text-slate-400 mr-1.5">Total Revenue:</span>
       <strong className="text-sm text-[#0078D4] dark:text-blue-400">
        ${totalRevenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
       </strong>
      </div>
      <div className={`px-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800/70 border-slate-700' : 'bg-slate-50 border-slate-200'}`}>
       <span className="text-slate-400 mr-1.5">Order Volume:</span>
       <strong className="text-sm">{orderCount} orders</strong>
      </div>
      <div className={`px-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800/70 border-slate-700' : 'bg-slate-50 border-slate-200'}`}>
       <span className="text-slate-400 mr-1.5">Avg Order Value (AOV):</span>
       <strong className="text-sm">
        ${avgOrderValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
       </strong>
      </div>
     </div>
    </div>

    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-5">
     {/* 1. Revenue Over Time */}
     <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-950/50 border-slate-800' : 'bg-slate-50/60 border-slate-200/70'}`}>
      <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
       Revenue Over Time
      </h4>
      <div className="h-48">
       <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={timeSeriesData}>
         <defs>
          <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
           <stop offset="5%" stopColor="#0078D4" stopOpacity={0.35} />
           <stop offset="95%" stopColor="#0078D4" stopOpacity={0.02} />
          </linearGradient>
         </defs>
         <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#e2e8f0'} />
         <XAxis dataKey="period" tick={{ fontSize: 10 }} />
         <YAxis tick={{ fontSize: 10 }} tickFormatter={v => `$${Math.round(v / 1000)}k`} />
         <RechartsTooltip formatter={(v: any) => [`$${Number(v).toLocaleString()}`, 'Revenue']} />
         <Area type="monotone" dataKey="revenue" stroke="#0078D4" fill="url(#revGrad)" strokeWidth={2} />
        </AreaChart>
       </ResponsiveContainer>
      </div>
     </div>

     {/* 2. Revenue by Region */}
     <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-950/50 border-slate-800' : 'bg-slate-50/60 border-slate-200/70'}`}>
      <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
       Revenue by Region
      </h4>
      <div className="h-48">
       <ResponsiveContainer width="100%" height="100%">
        <BarChart data={regionData} layout="vertical">
         <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#e2e8f0'} />
         <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={v => `$${Math.round(v / 1000)}k`} />
         <YAxis dataKey="region" type="category" width={88} tick={{ fontSize: 10 }} />
         <RechartsTooltip formatter={(v: any) => [`$${Number(v).toLocaleString()}`, 'Revenue']} />
         <Bar dataKey="revenue" fill="#4F46E5" radius={[0, 4, 4, 0]} />
        </BarChart>
       </ResponsiveContainer>
      </div>
     </div>

     {/* 3. Sales by Product Category */}
     <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-950/50 border-slate-800' : 'bg-slate-50/60 border-slate-200/70'}`}>
      <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
       Sales by Product Category
      </h4>
      <div className="h-48">
       <ResponsiveContainer width="100%" height="100%">
        <BarChart data={categoryData}>
         <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#e2e8f0'} />
         <XAxis dataKey="category" tick={{ fontSize: 9 }} interval={0} angle={-15} textAnchor="end" height={42} />
         <YAxis tick={{ fontSize: 10 }} tickFormatter={v => `$${Math.round(v / 1000)}k`} />
         <RechartsTooltip formatter={(v: any) => [`$${Number(v).toLocaleString()}`, 'Revenue']} />
         <Bar dataKey="revenue" fill="#0078D4" radius={[4, 4, 0, 0]} />
        </BarChart>
       </ResponsiveContainer>
      </div>
     </div>

     {/* 4. Customer Segment Distribution */}
     <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-950/50 border-slate-800' : 'bg-slate-50/60 border-slate-200/70'}`}>
      <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
       Customer Segment Share
      </h4>
      <div className="h-48">
       <ResponsiveContainer width="100%" height="100%">
        <PieChart>
         <Pie
          data={segmentData}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="50%"
          innerRadius={36}
          outerRadius={60}
         >
          {segmentData.map((_, i) => (
           <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
          ))}
         </Pie>
         <RechartsTooltip formatter={(v: any) => [`$${Number(v).toLocaleString()}`, 'Revenue']} />
         <Legend wrapperStyle={{ fontSize: '10px' }} />
        </PieChart>
       </ResponsiveContainer>
      </div>
     </div>
    </div>
   </div>

   {/* E. Recent Pipeline Activity & F. Data Engineering Alerts */}
   <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
    {/* E. Recent Pipeline Activity */}
    <div className={`lg:col-span-2 rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
     <div className="px-5 py-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
      <div>
       <h3 className="font-semibold text-sm">Recent Pipeline Executions</h3>
       <p className="text-xs text-slate-500 dark:text-slate-400">
        Real-time execution telemetry across ingestion and transformation jobs
       </p>
      </div>
      <button
       type="button"
       onClick={() => onNavigate('monitor')}
       className="text-xs font-medium text-[#0078D4] hover:underline cursor-pointer"
      >
       Open Pipeline Monitor →
      </button>
     </div>
     <div className="overflow-x-auto">
      <table className="w-full text-left border-collapse text-xs">
       <thead>
        <tr className={`border-b uppercase text-[10px] tracking-wider ${darkMode ? 'bg-slate-950/60 border-slate-800 text-slate-400' : 'bg-slate-50 border-slate-200/80 text-slate-500'}`}>
         <th className="py-3 px-4">Execution ID</th>
         <th className="py-3 px-4">Pipeline</th>
         <th className="py-3 px-4">Dataset</th>
         <th className="py-3 px-4">Trigger</th>
         <th className="py-3 px-4">Start Time</th>
         <th className="py-3 px-4 text-right">Duration</th>
         <th className="py-3 px-4 text-right">Records</th>
         <th className="py-3 px-4">Status</th>
        </tr>
       </thead>
       <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
        {executions.slice(0, 6).map(ex => (
         <tr
          key={ex.executionId}
          onClick={() => onNavigate('monitor')}
          className="hover:bg-slate-50/80 dark:hover:bg-slate-800/40 cursor-pointer transition-colors"
         >
          <td className="py-3 px-4 font-mono text-[#0078D4] dark:text-blue-400 font-medium">
           #{ex.executionId}
          </td>
          <td className="py-3 px-4 font-medium">{ex.pipelineName}</td>
          <td className="py-3 px-4 text-slate-500 dark:text-slate-400">{ex.datasetName}</td>
          <td className="py-3 px-4">{ex.triggerType}</td>
          <td className="py-3 px-4 text-slate-500">{ex.startTime}</td>
          <td className="py-3 px-4 text-right font-mono">{ex.totalDurationMs} ms</td>
          <td className="py-3 px-4 text-right">
           {ex.inputRowCount} → <strong>{ex.outputRowCount}</strong>
          </td>
          <td className="py-3 px-4">
           <StatusBadge status={ex.status} />
          </td>
         </tr>
        ))}
       </tbody>
      </table>
     </div>
    </div>

    {/* F. Data Engineering Alerts */}
    <div className={`rounded-xl border p-5 flex flex-col justify-between ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200/90 shadow-sm'}`}>
     <div>
      <div className="flex items-center justify-between mb-4">
       <div className="flex items-center gap-2">
        <ShieldAlert className="w-4 h-4 text-amber-500" />
        <h3 className="font-semibold text-sm">Data Engineering Alerts</h3>
       </div>
       <span className="text-xs font-semibold px-2 py-0.5 rounded-full bg-amber-500/10 text-amber-600 dark:text-amber-400 tabular-nums">
        {activeAlerts.length} active
       </span>
      </div>

      {activeAlerts.length === 0 ? (
       <div className="py-10 text-center">
        <div className="w-10 h-10 rounded-full bg-emerald-500/10 text-emerald-500 flex items-center justify-center mx-auto mb-2.5">
         <CheckCircle2 className="w-5 h-5" />
        </div>
        <div className="text-xs font-semibold">All Quality & Pipeline Checks Healthy</div>
        <p className="text-[11px] text-slate-500 mt-1 max-w-xs mx-auto">
         Zero unacknowledged schema mismatches, threshold violations, or ingestion failures.
        </p>
       </div>
      ) : (
       <div className="space-y-2.5 max-h-72 overflow-y-auto pr-1">
        {activeAlerts.map(alert => (
         <div
          key={alert.id}
          className={`p-3 rounded-xl border text-xs transition-all ${
           alert.severity === 'critical'
            ? 'bg-red-50/70 border-red-200 dark:bg-red-950/30 dark:border-red-900/60'
            : 'bg-amber-50/70 border-amber-200 dark:bg-amber-950/30 dark:border-amber-900/60'
          }`}
         >
          <div className="flex items-start justify-between gap-2">
           <span className="font-semibold text-slate-900 dark:text-slate-100">
            {alert.title}
           </span>
           <span className="text-[10px] font-mono text-slate-400 shrink-0">
            {alert.timestamp}
           </span>
          </div>
          <p className="text-[11px] text-slate-600 dark:text-slate-300 mt-1">
           {alert.description}
          </p>
          <div className="mt-2.5 flex items-center justify-between">
           <button
            type="button"
            onClick={() => onNavigate('quality')}
            className="text-[11px] font-semibold text-[#0078D4] hover:underline cursor-pointer"
           >
            Inspect Affected Rows
           </button>
           <button
            type="button"
            onClick={() => onAcknowledgeAlert(alert.id)}
            className="text-[11px] text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 cursor-pointer"
           >
            Acknowledge
           </button>
          </div>
         </div>
        ))}
       </div>
      )}
     </div>

     <div className="mt-4 pt-3 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between text-xs">
      <span className="text-slate-400">Simulate strict schema fault:</span>
      <button
       type="button"
       disabled={isExecuting}
       onClick={() => onRunPipeline(true)}
       className="px-2.5 py-1 rounded-lg border border-red-200 dark:border-red-800/70 text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-950/40 font-medium text-[11px] cursor-pointer"
      >
       Trigger Failure Drill
      </button>
     </div>
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 6. ACTIVITY FEED VIEW
// ============================================================================

const ActivityFeedView: React.FC<{
 activities: ActivityEvent[];
 onNavigate: (page: NavPageId) => void;
 darkMode: boolean;
}> = ({ activities, onNavigate, darkMode }) => {
 const [filterCat, setFilterCat] = useState<string>('ALL');

 const filtered = useMemo(() => {
  if (filterCat === 'ALL') return activities;
  return activities.filter(a => a.category === filterCat);
 }, [activities, filterCat]);

 return (
  <div className="space-y-5">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Platform Activity Feed</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Real-time audit trail of dataset uploads, schema validations, pipeline runs, and report exports.
     </p>
    </div>
    <div className="flex items-center gap-1.5 flex-wrap">
     {['ALL', 'Pipeline', 'Ingestion', 'Quality', 'Analytics', 'Architecture'].map(cat => (
      <button
       key={cat}
       type="button"
       onClick={() => setFilterCat(cat)}
       className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors cursor-pointer ${
        filterCat === cat
         ? 'bg-[#0078D4] text-white'
         : darkMode
         ? 'bg-slate-800 text-slate-300 hover:bg-slate-700'
         : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-50'
       }`}
      >
       {cat}
      </button>
     ))}
    </div>
   </div>

   <div className={`rounded-xl border divide-y ${darkMode ? 'bg-slate-900/90 border-slate-800 divide-slate-800' : 'bg-white border-slate-200 divide-slate-100 shadow-sm'}`}>
    {filtered.map(act => (
     <div key={act.id} className="p-4 flex items-start justify-between gap-4 hover:bg-slate-50/60 dark:hover:bg-slate-800/40 transition-colors">
      <div className="flex items-start gap-3">
       <div className={`mt-0.5 w-8 h-8 rounded-lg flex items-center justify-center shrink-0 ${
        act.status === 'success'
         ? 'bg-emerald-500/10 text-emerald-500'
         : act.status === 'error'
         ? 'bg-red-500/10 text-red-500'
         : act.status === 'warning'
         ? 'bg-amber-500/10 text-amber-500'
         : 'bg-blue-500/10 text-[#0078D4]'
       }`}>
        <Activity className="w-4 h-4" />
       </div>
       <div>
        <div className="flex items-center gap-2">
         <span className="font-semibold text-sm">{act.title}</span>
         <span className="text-[11px] px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-500">
          {act.category}
         </span>
        </div>
        <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">{act.detail}</p>
        <div className="text-[11px] text-slate-400 mt-1.5 font-mono tabular-nums">
         {act.timestamp} • Initiated by {act.actor}
        </div>
       </div>
      </div>
      <button
       type="button"
       onClick={() => onNavigate(act.targetPage)}
       className="text-xs font-medium text-[#0078D4] hover:underline shrink-0 cursor-pointer"
      >
       Inspect →
      </button>
     </div>
    ))}
   </div>
  </div>
 );
};

// ============================================================================
// 7. DATA SOURCES VIEW
// ============================================================================

const DataSourcesView: React.FC<{
 datasets: DatasetEntity[];
 activeDatasetId: string;
 onSelectDataset: (id: string) => void;
 onNavigate: (page: NavPageId) => void;
 onConnectSampleDatabase: () => void;
 darkMode: boolean;
}> = ({ datasets, activeDatasetId, onSelectDataset, onNavigate, onConnectSampleDatabase, darkMode }) => {
 const [showSqlModal, setShowSqlModal] = useState(false);
 const [sqlHost, setSqlHost] = useState('tcp:azurepulse-synapse-ondemand.sql.azuresynapse.net,1433');
 const [sqlDb, setSqlDb] = useState('commerce_operational_replica');
 const [sqlTable, setSqlTable] = useState('dbo.RegionalPartnerOrders_2026');

 const connectors = [
  {
   name: 'CSV File Connector',
   type: 'Local / Direct Upload',
   desc: 'RFC-4180 compliant parser with automatic delimiter detection, header inference, and null profiling.',
   status: 'Active Ready',
   actionLabel: 'Upload CSV File',
   onClick: () => onNavigate('ingestion')
  },
  {
   name: 'JSON Document Connector',
   type: 'Local / Direct Upload',
   desc: 'Ingests JSON record arrays or newline-delimited JSON objects directly into ADLS Gen2 Bronze storage.',
   status: 'Active Ready',
   actionLabel: 'Upload JSON File',
   onClick: () => onNavigate('ingestion')
  },
  {
   name: 'Synthetic Enterprise Generator',
   type: 'Built-in Demonstration Source',
   desc: 'Pre-configured e-commerce & cloud marketplace datasets containing realistic dirty-data edge cases.',
   status: 'Active Ready',
   actionLabel: 'Explore Datasets',
   onClick: () => onNavigate('explorer')
  },
  {
   name: 'Azure SQL / PostgreSQL Read Replica',
   type: 'Read-Only JDBC / TDS Interface',
   desc: 'Simulated read-only operational database connector supporting snapshot extraction into Bronze tables.',
   status: 'Simulated Mode',
   actionLabel: 'Configure Read Replica',
   onClick: () => setShowSqlModal(true)
  },
  {
   name: 'Azure Data Lake Storage Gen2 (ADLS)',
   type: 'abfss:// Container Mount',
   desc: 'Hierarchical namespace storage for raw landing zones (Bronze) and Delta Lake tables (Silver/Gold).',
   status: 'Simulated Mode',
   actionLabel: 'Configure Integration',
   onClick: () => onNavigate('integrations')
  }
 ];

 return (
  <div className="space-y-6">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Data Sources & Connectors</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Manage registered datasets and configure file, database, and Azure Data Lake ingestion sources.
     </p>
    </div>
    <button
     type="button"
     onClick={() => onNavigate('ingestion')}
     className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] hover:bg-blue-700 text-white shadow-sm cursor-pointer"
    >
     <Plus className="w-4 h-4" />
     New Data Ingestion Job
    </button>
   </div>

   {/* Supported Connectors Grid */}
   <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
    {connectors.map(conn => (
     <div
      key={conn.name}
      className={`rounded-xl border p-4 flex flex-col justify-between ${
       darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'
      }`}
     >
      <div>
       <div className="flex items-center justify-between mb-2">
        <HardDrive className="w-4 h-4 text-[#0078D4]" />
        <StatusBadge status={conn.status} />
       </div>
       <h3 className="font-semibold text-sm">{conn.name}</h3>
       <div className="text-[11px] text-[#0078D4] dark:text-blue-400 font-medium mt-0.5">{conn.type}</div>
       <p className="text-xs text-slate-500 dark:text-slate-400 mt-2 leading-relaxed">{conn.desc}</p>
      </div>
      <button
       type="button"
       onClick={conn.onClick}
       className={`mt-4 w-full py-1.5 px-3 rounded-lg border text-xs font-medium transition-colors cursor-pointer ${
        darkMode
         ? 'border-slate-700 hover:bg-slate-800 text-slate-200'
         : 'border-slate-200 hover:bg-slate-50 text-slate-700'
       }`}
      >
       {conn.actionLabel}
      </button>
     </div>
    ))}
   </div>

   {/* Registered Datasets Table */}
   <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div className="px-5 py-4 border-b border-slate-100 dark:border-slate-800">
     <h3 className="font-semibold text-sm">Registered Platform Datasets</h3>
     <p className="text-xs text-slate-500 dark:text-slate-400">
      Click any dataset to make it active across the Pipeline Builder, Data Quality Engine, and Analytics Workspace.
     </p>
    </div>
    <div className="overflow-x-auto">
     <table className="w-full text-left border-collapse text-xs">
      <thead>
       <tr className={`border-b uppercase text-[10px] tracking-wider ${darkMode ? 'bg-slate-950/60 border-slate-800 text-slate-400' : 'bg-slate-50 border-slate-200 text-slate-500'}`}>
        <th className="py-3 px-4">Dataset Name</th>
        <th className="py-3 px-4">Source Type</th>
        <th className="py-3 px-4">Destination Layer</th>
        <th className="py-3 px-4 text-right">Raw Rows</th>
        <th className="py-3 px-4 text-right">Curated Rows</th>
        <th className="py-3 px-4 text-right">Raw → Curated Quality</th>
        <th className="py-3 px-4">Status</th>
        <th className="py-3 px-4 text-right">Actions</th>
       </tr>
      </thead>
      <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
       {datasets.map(ds => {
        const isSelected = ds.id === activeDatasetId;
        return (
         <tr
          key={ds.id}
          className={`${isSelected ? 'bg-blue-50/50 dark:bg-blue-950/20' : ''} hover:bg-slate-50 dark:hover:bg-slate-800/40 transition-colors`}
         >
          <td className="py-3.5 px-4 font-semibold flex items-center gap-2">
           <FileSpreadsheet className="w-4 h-4 text-[#0078D4]" />
           <span>{ds.name}</span>
           {isSelected && (
            <span className="text-[10px] px-2 py-0.5 rounded bg-[#0078D4] text-white font-medium">
             Active
            </span>
           )}
          </td>
          <td className="py-3.5 px-4">{ds.sourceType}</td>
          <td className="py-3.5 px-4 font-mono text-[11px]">{ds.ingestionConfig.destinationLayer}</td>
          <td className="py-3.5 px-4 text-right">{ds.rawRows.length}</td>
          <td className="py-3.5 px-4 text-right font-semibold">{ds.curatedRows.length}</td>
          <td className="py-3.5 px-4 text-right">
           <span className="text-amber-600 dark:text-amber-400">{ds.rawQuality.overallScore}%</span>
           <span className="mx-1.5 text-slate-400">→</span>
           <strong className="text-emerald-600 dark:text-emerald-400">{ds.curatedQuality.overallScore}%</strong>
          </td>
          <td className="py-3.5 px-4">
           <StatusBadge status={ds.status} />
          </td>
          <td className="py-3.5 px-4 text-right space-x-2">
           <button
            type="button"
            onClick={() => {
             onSelectDataset(ds.id);
             onNavigate('explorer');
            }}
            className="text-xs font-medium text-[#0078D4] hover:underline cursor-pointer"
           >
            Explore
           </button>
           <button
            type="button"
            onClick={() => {
             onSelectDataset(ds.id);
             onNavigate('quality');
            }}
            className="text-xs font-medium text-indigo-600 dark:text-indigo-400 hover:underline cursor-pointer"
           >
            Quality
           </button>
          </td>
         </tr>
        );
       })}
      </tbody>
     </table>
    </div>
   </div>

   {/* Read-Only SQL Connector Modal */}
   {showSqlModal && (
    <div className="fixed inset-0 z-50 bg-slate-950/60 backdrop-blur-sm flex items-center justify-center p-4">
     <div className={`w-full max-w-lg rounded-2xl border p-6 shadow-xl ${darkMode ? 'bg-slate-900 border-slate-800 text-slate-100' : 'bg-white border-slate-200 text-slate-900'}`}>
      <div className="flex items-center justify-between mb-4">
       <div className="flex items-center gap-2">
        <Database className="w-5 h-5 text-[#0078D4]" />
        <h3 className="font-bold text-base">Read-Only Database Connector (Demo Simulation)</h3>
       </div>
       <button type="button" onClick={() => setShowSqlModal(false)} className="p-1 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer">
        <X className="w-4 h-4" />
       </button>
      </div>
      <p className="text-xs text-slate-500 dark:text-slate-400 mb-4">
       Simulate extracting a read-only snapshot from an Azure SQL or PostgreSQL operational replica into ADLS Gen2 Bronze storage.
      </p>
      <div className="space-y-3 text-xs">
       <div>
        <label className="block font-medium mb-1">Server Endpoint (Read-Only Intent)</label>
        <input
         type="text"
         value={sqlHost}
         onChange={e => setSqlHost(e.target.value)}
         className={`w-full px-3 py-2 rounded-lg border font-mono text-xs ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        />
       </div>
       <div className="grid grid-cols-2 gap-3">
        <div>
         <label className="block font-medium mb-1">Database Name</label>
         <input
          type="text"
          value={sqlDb}
          onChange={e => setSqlDb(e.target.value)}
          className={`w-full px-3 py-2 rounded-lg border font-mono text-xs ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
         />
        </div>
        <div>
         <label className="block font-medium mb-1">Source Table / View</label>
         <input
          type="text"
          value={sqlTable}
          onChange={e => setSqlTable(e.target.value)}
          className={`w-full px-3 py-2 rounded-lg border font-mono text-xs ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
         />
        </div>
       </div>
       <div className="p-3 rounded-lg bg-blue-500/10 border border-blue-500/20 text-[#0078D4] dark:text-blue-300 text-[11px]">
        Authentication Mode: <strong>Managed Identity (SystemAssignedMSI)</strong> — ApplicationIntent=ReadOnly verified.
       </div>
      </div>
      <div className="mt-5 flex items-center justify-end gap-2">
       <button
        type="button"
        onClick={() => setShowSqlModal(false)}
        className="px-3.5 py-2 rounded-lg text-xs font-medium border border-slate-200 dark:border-slate-700 cursor-pointer"
       >
        Cancel
       </button>
       <button
        type="button"
        onClick={() => {
         onConnectSampleDatabase();
         setShowSqlModal(false);
        }}
        className="px-4 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] text-white hover:bg-blue-700 cursor-pointer"
       >
        Ingest Read-Only Snapshot
       </button>
      </div>
     </div>
    </div>
   )}
  </div>
 );
};

// ============================================================================
// 8. DATA INGESTION MODULE (DRAG-AND-DROP CSV/JSON UPLOAD + CONFIG + PREVIEW)
// ============================================================================

interface DataIngestionViewProps {
 onIngestNewDataset: (
  rawRows: DataRow[],
  config: IngestionConfig,
  runPipelineImmediately: boolean
 ) => void;
 darkMode: boolean;
}

const DataIngestionView: React.FC<DataIngestionViewProps> = ({
 onIngestNewDataset,
 darkMode
}) => {
 const [dragActive, setDragActive] = useState(false);
 const [uploadError, setUploadError] = useState<string | null>(null);
 const [uploadProgress, setUploadProgress] = useState<number>(0);
 const [uploadedFileName, setUploadedFileName] = useState<string>('partner_cloud_orders_q3.csv (Ready to test or upload your own)');
 const [previewRows, setPreviewRows] = useState<DataRow[]>(() => parseUploadedCSV(SAMPLE_UPLOAD_CSV_TEXT));

 // Ingestion configuration state
 const [sourceName, setSourceName] = useState('ADF_Partner_Orders_Upload');
 const [sourceType, setSourceType] = useState<IngestionConfig['sourceType']>('CSV Upload');
 const [datasetName, setDatasetName] = useState('Partner Cloud Orders Q3 2026');
 const [destinationLayer, setDestinationLayer] = useState<IngestionConfig['destinationLayer']>('raw_bronze');
 const [schemaStrategy, setSchemaStrategy] = useState<IngestionConfig['schemaStrategy']>('Infer & Evolve');
 const [duplicateHandling, setDuplicateHandling] = useState<IngestionConfig['duplicateHandling']>('Remove Exact & Key Duplicates');
 const [nullHandling, setNullHandling] = useState<IngestionConfig['nullHandling']>('Impute Median / Defaults');
 const [dateParsingRule, setDateParsingRule] = useState<IngestionConfig['dateParsingRule']>('ISO YYYY-MM-DD (Drop Invalid)');

 const fileInputRef = useRef<HTMLInputElement | null>(null);

 const inferredSchema = useMemo(() => inferDatasetSchema(previewRows), [previewRows]);
 const previewQuality = useMemo(() => evaluateDataQuality(previewRows), [previewRows]);

 const [colMappings, setColMappings] = useState<{ sourceCol: string; targetCol: string; targetType: string; required: boolean }[]>([]);

 useEffect(() => {
  setColMappings(
   inferredSchema.map(col => ({
    sourceCol: col.name,
    targetCol: col.name,
    targetType: col.inferredType,
    required: ['order_id', 'order_date', 'product_name', 'category', 'quantity', 'unit_price'].includes(col.name)
   }))
  );
 }, [inferredSchema]);

 const handleFileContent = (file: File) => {
  setUploadError(null);
  const ext = file.name.split('.').pop()?.toLowerCase();
  if (ext !== 'csv' && ext !== 'json') {
   setUploadError(`Unsupported file format ".${ext}". Please upload a valid .csv or .json dataset.`);
   return;
  }
  if (file.size > 15 * 1024 * 1024) {
   setUploadError('File exceeds maximum demo upload limit of 15 MB.');
   return;
  }

  setUploadProgress(25);
  const reader = new FileReader();
  reader.onload = e => {
   setUploadProgress(75);
   try {
    const text = String(e.target?.result || '');
    let parsed: DataRow[] = [];
    if (ext === 'json') {
     const jsonObj = JSON.parse(text);
     parsed = Array.isArray(jsonObj) ? jsonObj : jsonObj.records || jsonObj.data || [];
     setSourceType('JSON Upload');
    } else {
     parsed = parseUploadedCSV(text);
     setSourceType('CSV Upload');
    }

    if (!parsed || parsed.length === 0) {
     setUploadError('No valid data records found in uploaded file. Check header and row structure.');
     setUploadProgress(0);
     return;
    }

    setPreviewRows(parsed);
    setUploadedFileName(`${file.name} (${(file.size / 1024).toFixed(1)} KB)`);
    const baseName = file.name.replace(/\.(csv|json)$/i, '').replace(/[_-]+/g, ' ');
    setDatasetName(baseName.charAt(0).toUpperCase() + baseName.slice(1));
    setSourceName(`ADF_${file.name.replace(/[^a-zA-Z0-9]/g, '_')}`);
    setUploadProgress(100);
   } catch (err: any) {
    setUploadError(`Failed to parse file: ${err?.message || 'Malformed syntax'}`);
    setUploadProgress(0);
   }
  };
  reader.readAsText(file);
 };

 const handleDrop = (e: React.DragEvent) => {
  e.preventDefault();
  setDragActive(false);
  if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
   handleFileContent(e.dataTransfer.files[0]);
  }
 };

 const handleLoadSampleCSV = () => {
  setUploadError(null);
  setUploadProgress(100);
  const rows = parseUploadedCSV(SAMPLE_UPLOAD_CSV_TEXT);
  setPreviewRows(rows);
  setUploadedFileName('sample_partner_orders_dirty.csv (1.2 KB)');
  setDatasetName('Partner Cloud Orders Q3 (Dirty Sample)');
  setSourceName('ADF_Sample_Partner_CSV');
  setSourceType('CSV Upload');
 };

 const handleCommitIngestion = (runImmediately: boolean) => {
  const config: IngestionConfig = {
   sourceName,
   sourceType,
   datasetName: datasetName.trim() || 'Uploaded Dataset',
   destinationLayer,
   schemaStrategy,
   duplicateHandling,
   nullHandling,
   dateParsingRule,
   columnMappings: colMappings
  };
  onIngestNewDataset(previewRows, config, runImmediately);
 };

 const totalMissingCells = useMemo(
  () => inferredSchema.reduce((acc, c) => acc + c.nullCount, 0),
  [inferredSchema]
 );

 return (
  <div className="space-y-6">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Data Ingestion & Schema Profiler</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Upload CSV or JSON datasets without Azure credentials, inspect inferred schema & quality edge cases, and configure ingestion rules.
     </p>
    </div>
    <div className="flex items-center gap-2">
     <button
      type="button"
      onClick={() => triggerFileDownload(SAMPLE_UPLOAD_CSV_TEXT, 'sample_azurepulse_orders.csv')}
      className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs font-medium cursor-pointer ${
       darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
      }`}
     >
      <Download className="w-3.5 h-3.5 text-[#0078D4]" />
      Download Sample CSV
     </button>
     <button
      type="button"
      onClick={handleLoadSampleCSV}
      className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs font-medium cursor-pointer ${
       darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
      }`}
     >
      <RefreshCw className="w-3.5 h-3.5 text-indigo-500" />
      Reset to Sample Dirty CSV
     </button>
    </div>
   </div>

   {/* Drag and Drop Upload Area */}
   <div
    onDragOver={e => { e.preventDefault(); setDragActive(true); }}
    onDragLeave={() => setDragActive(false)}
    onDrop={handleDrop}
    onClick={() => fileInputRef.current?.click()}
    className={`rounded-2xl border-2 border-dashed p-7 text-center transition-all cursor-pointer ${
     dragActive
      ? 'border-[#0078D4] bg-blue-50/70 dark:bg-blue-950/30'
      : darkMode
      ? 'border-slate-700 bg-slate-900/60 hover:border-slate-600'
      : 'border-slate-300 bg-white hover:border-[#0078D4]/60 shadow-sm'
    }`}
   >
    <input
     ref={fileInputRef}
     type="file"
     accept=".csv,.json"
     onChange={e => e.target.files?.[0] && handleFileContent(e.target.files[0])}
     className="hidden"
    />
    <div className="w-12 h-12 rounded-2xl bg-[#0078D4]/10 text-[#0078D4] flex items-center justify-center mx-auto mb-3">
     <Upload className="w-6 h-6" />
    </div>
    <div className="font-semibold text-sm">
     Drag and drop your CSV or JSON dataset here, or <span className="text-[#0078D4] underline">browse files</span>
    </div>
    <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">
     Supports RFC-4180 `.csv` and `.json` record arrays up to 15 MB. Automatic column detection, type inference & missing-value profiling.
    </p>
    <div className="mt-3 inline-flex items-center gap-2 px-3 py-1 rounded-full bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 text-xs font-medium">
     <CheckCircle2 className="w-3.5 h-3.5" />
     <span>Loaded in Staging Buffer: {uploadedFileName}</span>
    </div>
    {uploadProgress > 0 && uploadProgress < 100 && (
     <div className="w-64 h-1.5 bg-slate-200 rounded-full mx-auto mt-3 overflow-hidden">
      <div className="h-full bg-[#0078D4]" style={{ width: `${uploadProgress}%` }} />
     </div>
    )}
   </div>

   {uploadError && (
    <div className="p-4 rounded-xl bg-red-50 border border-red-200 text-red-700 dark:bg-red-950/50 dark:border-red-800 dark:text-red-300 text-xs flex items-center gap-2.5">
     <AlertCircle className="w-4 h-4 shrink-0" />
     <span>{uploadError}</span>
    </div>
   )}

   {/* Detected File Summary Bar */}
   <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
    <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[11px] text-slate-400">Detected Rows</div>
     <div className="text-xl font-bold tabular-nums mt-0.5">{previewRows.length}</div>
    </div>
    <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[11px] text-slate-400">Detected Columns</div>
     <div className="text-xl font-bold tabular-nums mt-0.5">{inferredSchema.length}</div>
    </div>
    <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[11px] text-slate-400">Missing Values</div>
     <div className="text-xl font-bold tabular-nums text-amber-500 mt-0.5">{totalMissingCells} cells</div>
    </div>
    <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[11px] text-slate-400">Duplicate Keys</div>
     <div className="text-xl font-bold tabular-nums text-amber-500 mt-0.5">{previewQuality.duplicateRows} rows</div>
    </div>
    <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[11px] text-slate-400">Raw Quality Score</div>
     <div className="text-xl font-bold tabular-nums text-[#0078D4] mt-0.5">{previewQuality.overallScore}%</div>
    </div>
   </div>

   {/* Two-Column Layout: Left = Ingestion Configuration, Right = Inferred Schema & Data Preview */}
   <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
    {/* Ingestion Configuration Form */}
    <div className={`lg:col-span-5 rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="flex items-center gap-2 pb-3 border-b border-slate-100 dark:border-slate-800">
      <Sliders className="w-4 h-4 text-[#0078D4]" />
      <h3 className="font-semibold text-sm">Ingestion Job Configuration</h3>
     </div>

     <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
      <div>
       <label className="block font-medium mb-1">Source Name</label>
       <input
        type="text"
        value={sourceName}
        onChange={e => setSourceName(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>
      <div>
       <label className="block font-medium mb-1">Dataset Name</label>
       <input
        type="text"
        value={datasetName}
        onChange={e => setDatasetName(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>
      <div>
       <label className="block font-medium mb-1">Source Type</label>
       <select
        value={sourceType}
        onChange={e => setSourceType(e.target.value as any)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value="CSV Upload">CSV Upload</option>
        <option value="JSON Upload">JSON Upload</option>
        <option value="Synthetic Dataset">Synthetic Dataset</option>
        <option value="Azure ADLS Gen2">Azure ADLS Gen2</option>
       </select>
      </div>
      <div>
       <label className="block font-medium mb-1">Destination Layer</label>
       <select
        value={destinationLayer}
        onChange={e => setDestinationLayer(e.target.value as any)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value="raw_bronze">ADLS Gen2 — raw_bronze</option>
        <option value="staging_silver">ADLS Gen2 — staging_silver</option>
        <option value="curated_gold">ADLS Gen2 — curated_gold</option>
       </select>
      </div>
      <div>
       <label className="block font-medium mb-1">Schema Handling Strategy</label>
       <select
        value={schemaStrategy}
        onChange={e => setSchemaStrategy(e.target.value as any)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value="Infer & Evolve">Infer & Evolve Schema</option>
        <option value="Strict Enforcement">Strict Schema Enforcement</option>
        <option value="Quarantine Invalid">Quarantine Invalid Records</option>
       </select>
      </div>
      <div>
       <label className="block font-medium mb-1">Duplicate Handling</label>
       <select
        value={duplicateHandling}
        onChange={e => setDuplicateHandling(e.target.value as any)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value="Remove Exact & Key Duplicates">Remove Exact & Key Duplicates</option>
        <option value="Remove Exact Only">Remove Exact Duplicates Only</option>
        <option value="Retain All">Retain All Records</option>
       </select>
      </div>
      <div>
       <label className="block font-medium mb-1">Null Value Handling</label>
       <select
        value={nullHandling}
        onChange={e => setNullHandling(e.target.value as any)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value="Impute Median / Defaults">Impute Median / Defaults</option>
        <option value="Drop Rows with Nulls">Drop Rows with Nulls</option>
        <option value="Retain Nulls">Retain Nulls</option>
       </select>
      </div>
      <div>
       <label className="block font-medium mb-1">Date Parsing Rule</label>
       <select
        value={dateParsingRule}
        onChange={e => setDateParsingRule(e.target.value as any)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value="ISO YYYY-MM-DD (Drop Invalid)">ISO YYYY-MM-DD (Quarantine Invalid)</option>
        <option value="Coerce Invalid to Fallback Date">Coerce Invalid to Fallback Date</option>
       </select>
      </div>
     </div>

     {/* Column Mapping Table */}
     <div className="pt-2">
      <div className="text-xs font-semibold mb-2">Column Mappings & Type Inference</div>
      <div className="max-h-48 overflow-y-auto rounded-lg border border-slate-200 dark:border-slate-800">
       <table className="w-full text-left text-[11px]">
        <thead className={darkMode ? 'bg-slate-950 text-slate-400' : 'bg-slate-50 text-slate-500'}>
         <tr>
          <th className="py-2 px-2.5">Source Col</th>
          <th className="py-2 px-2.5">Target Col</th>
          <th className="py-2 px-2.5">Type</th>
          <th className="py-2 px-2.5 text-center">Req</th>
         </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
         {colMappings.map((m, i) => (
          <tr key={m.sourceCol}>
           <td className="py-1.5 px-2.5 font-mono">{m.sourceCol}</td>
           <td className="py-1.5 px-2.5">
            <input
             type="text"
             value={m.targetCol}
             onChange={e => {
              const next = [...colMappings];
              next[i] = { ...next[i], targetCol: e.target.value };
              setColMappings(next);
             }}
             className={`w-28 px-1.5 py-0.5 rounded border font-mono text-[11px] ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-white border-slate-200'}`}
            />
           </td>
           <td className="py-1.5 px-2.5 font-mono text-[#0078D4] dark:text-blue-400">{m.targetType}</td>
           <td className="py-1.5 px-2.5 text-center">
            <input
             type="checkbox"
             checked={m.required}
             onChange={e => {
              const next = [...colMappings];
              next[i] = { ...next[i], required: e.target.checked };
              setColMappings(next);
             }}
            />
           </td>
          </tr>
         ))}
        </tbody>
       </table>
      </div>
     </div>

     <div className="pt-2 flex flex-col sm:flex-row gap-2.5">
      <button
       type="button"
       onClick={() => handleCommitIngestion(true)}
       className="flex-1 py-2.5 px-4 rounded-xl bg-[#0078D4] hover:bg-blue-700 text-white font-semibold text-xs flex items-center justify-center gap-2 shadow-sm cursor-pointer"
      >
       <Play className="w-4 h-4" />
       Ingest & Run Pipeline Now
      </button>
     </div>
    </div>

    {/* Right Column: Inferred Schema Summary & Incoming Dataset Preview */}
    <div className="lg:col-span-7 space-y-5">
     {/* Schema & Missing Value Profile */}
     <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
      <div className="px-5 py-3.5 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
       <h3 className="font-semibold text-sm">Inferred Column Schema & Missing-Value Summary</h3>
       <span className="text-xs text-slate-400">{inferredSchema.length} columns profiled</span>
      </div>
      <div className="overflow-x-auto">
       <table className="w-full text-left text-xs">
        <thead className={`uppercase text-[10px] ${darkMode ? 'bg-slate-950/60 text-slate-400' : 'bg-slate-50 text-slate-500'}`}>
         <tr>
          <th className="py-2.5 px-4">Column</th>
          <th className="py-2.5 px-4">Inferred Type</th>
          <th className="py-2.5 px-4 text-right">Null Count</th>
          <th className="py-2.5 px-4 text-right">Unique Values</th>
          <th className="py-2.5 px-4">Sample Values</th>
         </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
         {inferredSchema.map(col => (
          <tr key={col.name}>
           <td className="py-2.5 px-4 font-mono font-medium">{col.name}</td>
           <td className="py-2.5 px-4">
            <span className="px-2 py-0.5 rounded bg-blue-500/10 text-[#0078D4] dark:text-blue-400 font-mono text-[11px]">
             {col.inferredType}
            </span>
           </td>
           <td className={`py-2.5 px-4 text-right font-semibold ${col.nullCount > 0 ? 'text-amber-500' : 'text-emerald-500'}`}>
            {col.nullCount}
           </td>
           <td className="py-2.5 px-4 text-right">{col.uniqueCount}</td>
           <td className="py-2.5 px-4 text-slate-500 dark:text-slate-400 truncate max-w-xs">
            {col.sampleValues.join(', ')}
           </td>
          </tr>
         ))}
        </tbody>
       </table>
      </div>
     </div>

     {/* Incoming Raw Dataset Preview */}
     <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
      <div className="px-5 py-3.5 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
       <div>
        <h3 className="font-semibold text-sm">Incoming Raw Dataset Preview (First 10 Rows)</h3>
        <p className="text-[11px] text-slate-500">
         Anomalies (invalid dates, negative numbers, missing fields, unnormalized categories) are highlighted in amber/red.
        </p>
       </div>
      </div>
      <div className="overflow-x-auto">
       <table className="w-full text-left text-xs">
        <thead className={`uppercase text-[10px] ${darkMode ? 'bg-slate-950/60 text-slate-400' : 'bg-slate-50 text-slate-500'}`}>
         <tr>
          <th className="py-2.5 px-3">order_id</th>
          <th className="py-2.5 px-3">order_date</th>
          <th className="py-2.5 px-3">product_name</th>
          <th className="py-2.5 px-3">category</th>
          <th className="py-2.5 px-3 text-right">quantity</th>
          <th className="py-2.5 px-3 text-right">unit_price</th>
          <th className="py-2.5 px-3">region</th>
          <th className="py-2.5 px-3">segment</th>
         </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
         {previewRows.slice(0, 10).map((r, i) => {
          const badDate = !isValidISODate(r.order_date);
          const badQty = Number(r.quantity) <= 0 || isNaN(Number(r.quantity));
          const badPrice = r.unit_price === null || r.unit_price === undefined || Number(r.unit_price) <= 0;
          const badCat = String(r.category || '') !== normalizeCategoryLabel(r.category);
          return (
           <tr key={i} className="hover:bg-slate-50/70 dark:hover:bg-slate-800/40">
            <td className="py-2 px-3 font-mono font-medium">{r.order_id}</td>
            <td className={`py-2 px-3 font-mono ${badDate ? 'text-red-600 font-bold bg-red-500/10' : ''}`}>
             {String(r.order_date ?? 'NULL')}
            </td>
            <td className="py-2 px-3 truncate max-w-[160px]">{r.product_name}</td>
            <td className={`py-2 px-3 font-mono ${badCat ? 'text-amber-600 bg-amber-500/10' : ''}`}>
             "{String(r.category ?? '')}"
            </td>
            <td className={`py-2 px-3 text-right font-mono ${badQty ? 'text-red-600 font-bold bg-red-500/10' : ''}`}>
             {String(r.quantity ?? 'NULL')}
            </td>
            <td className={`py-2 px-3 text-right font-mono ${badPrice ? 'text-red-600 font-bold bg-red-500/10' : ''}`}>
             {r.unit_price === null || r.unit_price === undefined ? 'NULL' : `$${r.unit_price}`}
            </td>
            <td className={`py-2 px-3 ${!r.region ? 'text-amber-600 font-semibold bg-amber-500/10' : ''}`}>
             {r.region || 'NULL'}
            </td>
            <td className={`py-2 px-3 ${!r.customer_segment ? 'text-amber-600 font-semibold bg-amber-500/10' : ''}`}>
             {r.customer_segment || 'NULL'}
            </td>
           </tr>
          );
         })}
        </tbody>
       </table>
      </div>
     </div>
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 9. PIPELINE BUILDER & TRANSFORMATION ENGINE WORKSPACE
// ============================================================================

interface PipelineBuilderViewProps {
 pipeline: PipelineDefinition;
 savedPipelines: PipelineDefinition[];
 onSelectSavedPipeline: (id: string) => void;
 onSavePipeline: (updated: PipelineDefinition) => void;
 onRunCustomPipeline: (updated: PipelineDefinition, simulateFailure?: boolean) => void;
 activeDataset: DatasetEntity;
 isExecuting: boolean;
 darkMode: boolean;
}

const NODE_PALETTE: { type: NodeType; label: string; subtitle: string; service: string; color: string }[] = [
 { type: 'SOURCE', label: 'Source Dataset', subtitle: 'CSV / JSON Reader', service: 'ADLS Landing', color: 'border-blue-500' },
 { type: 'INGEST', label: 'Ingest Layer', subtitle: 'Bronze Table Loader', service: 'Azure Data Factory', color: 'border-sky-500' },
 { type: 'VALIDATE', label: 'Schema & Quality', subtitle: 'Validation Gate', service: 'Quality Engine', color: 'border-amber-500' },
 { type: 'DEDUPLICATE', label: 'Deduplicate', subtitle: 'Exact & Key Dedup', service: 'Databricks Spark', color: 'border-purple-500' },
 { type: 'TRANSFORM', label: 'Clean & Feature Eng', subtitle: 'Normalize & Derive', service: 'Databricks PySpark', color: 'border-indigo-500' },
 { type: 'FILTER', label: 'Conditional Filter', subtitle: 'Predicate Pushdown', service: 'Databricks Spark', color: 'border-cyan-500' },
 { type: 'AGGREGATE', label: 'Group & Aggregate', subtitle: 'Rollup Metrics', service: 'Databricks Spark', color: 'border-pink-500' },
 { type: 'CURATED_STORAGE', label: 'Curated Storage', subtitle: 'Delta Gold Table', service: 'ADLS Gen2 Gold', color: 'border-emerald-500' },
 { type: 'ANALYTICS', label: 'Analytics Sink', subtitle: 'BI Semantic Model', service: 'Synapse SQL', color: 'border-teal-500' }
];

const NODE_STAGE_ORDER: Record<NodeType, number> = {
 SOURCE: 1,
 INGEST: 2,
 VALIDATE: 3,
 DEDUPLICATE: 4,
 TRANSFORM: 5,
 FILTER: 6,
 AGGREGATE: 7,
 CURATED_STORAGE: 8,
 ANALYTICS: 9
};

const PipelineBuilderView: React.FC<PipelineBuilderViewProps> = ({
 pipeline,
 savedPipelines,
 onSelectSavedPipeline,
 onSavePipeline,
 onRunCustomPipeline,
 activeDataset,
 isExecuting,
 darkMode
}) => {
 const [nodes, setNodes] = useState<PipelineNodeData[]>(pipeline.nodes);
 const [edges, setEdges] = useState<PipelineEdgeData[]>(pipeline.edges);
 const [transformConfig, setTransformConfig] = useState<TransformationRuleConfig>(pipeline.transformConfig);
 const [pipelineName, setPipelineName] = useState<string>(pipeline.name);
 const [selectedNodeId, setSelectedNodeId] = useState<string>(pipeline.nodes[4]?.id || pipeline.nodes[0]?.id || '');
 const [connectingFromId, setConnectingFromId] = useState<string | null>(null);
 const [zoom, setZoom] = useState<number>(1);
 const [pan, setPan] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
 const [draggingNodeId, setDraggingNodeId] = useState<string | null>(null);
 const [dragOffset, setDragOffset] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
 const [validationResult, setValidationResult] = useState<{ valid: boolean; messages: string[] } | null>(null);
 const [selectedStepDiffId, setSelectedStepDiffId] = useState<string>('step-clean');

 const canvasRef = useRef<HTMLDivElement | null>(null);

 useEffect(() => {
  setNodes(pipeline.nodes);
  setEdges(pipeline.edges);
  setTransformConfig(pipeline.transformConfig);
  setPipelineName(pipeline.name);
 }, [pipeline]);

 // Live preview of transformation engine based on current builder settings
 const livePreviewResult = useMemo(
  () => executeTransformationPipeline(activeDataset.rawRows, transformConfig),
  [activeDataset.rawRows, transformConfig]
 );

 const selectedNode = useMemo(
  () => nodes.find(n => n.id === selectedNodeId) || nodes[0] || null,
  [nodes, selectedNodeId]
 );

 const validateCurrentGraph = useCallback((): { valid: boolean; messages: string[] } => {
  const msgs: string[] = [];
  const hasSource = nodes.some(n => n.type === 'SOURCE');
  const hasCurated = nodes.some(n => n.type === 'CURATED_STORAGE' || n.type === 'ANALYTICS');
  if (!hasSource) {
   msgs.push('Missing SOURCE node: Pipeline must start with a valid data source.');
  }
  if (!hasCurated) {
   msgs.push('Missing sink node: Add CURATED_STORAGE or ANALYTICS node to persist output.');
  }

  // Check for disconnected nodes
  if (nodes.length > 1) {
   const connectedIds = new Set<string>();
   edges.forEach(e => {
    connectedIds.add(e.source);
    connectedIds.add(e.target);
   });
   const disconnected = nodes.filter(n => !connectedIds.has(n.id));
   if (disconnected.length > 0) {
    msgs.push(`Disconnected nodes detected: ${disconnected.map(d => d.label).join(', ')}. Connect or remove them before running.`);
   }
  }

  // Check execution order on edges
  edges.forEach(e => {
   const src = nodes.find(n => n.id === e.source);
   const tgt = nodes.find(n => n.id === e.target);
   if (src && tgt && NODE_STAGE_ORDER[src.type] > NODE_STAGE_ORDER[tgt.type]) {
    msgs.push(`Invalid backward connection: ${src.label} (${src.type}) cannot feed upstream into ${tgt.label} (${tgt.type}).`);
   }
  });

  // Check filter condition validity if enabled
  if (transformConfig.filterEnabled && !transformConfig.filterValue && transformConfig.filterOperator !== 'not_null') {
   msgs.push('Filter node is enabled but Filter Threshold/Value is empty.');
  }

  return { valid: msgs.length === 0, messages: msgs };
 }, [nodes, edges, transformConfig]);

 const handleNodeMouseDown = (e: React.MouseEvent, node: PipelineNodeData) => {
  e.stopPropagation();
  setSelectedNodeId(node.id);
  if (!canvasRef.current) return;
  const rect = canvasRef.current.getBoundingClientRect();
  setDraggingNodeId(node.id);
  setDragOffset({
   x: (e.clientX - rect.left - pan.x) / zoom - node.x,
   y: (e.clientY - rect.top - pan.y) / zoom - node.y
  });
 };

 const handleCanvasMouseMove = (e: React.MouseEvent) => {
  if (!draggingNodeId || !canvasRef.current) return;
  const rect = canvasRef.current.getBoundingClientRect();
  const newX = Math.max(16, Math.min(1150, Math.round((e.clientX - rect.left - pan.x) / zoom - dragOffset.x)));
  const newY = Math.max(16, Math.min(360, Math.round((e.clientY - rect.top - pan.y) / zoom - dragOffset.y)));
  setNodes(prev => prev.map(n => (n.id === draggingNodeId ? { ...n, x: newX, y: newY } : n)));
 };

 const handleCanvasMouseUp = () => {
  setDraggingNodeId(null);
 };

 const handleAddNode = (template: typeof NODE_PALETTE[number]) => {
  const newId = `node-${Date.now().toString().slice(-4)}`;
  const maxX = nodes.reduce((m, n) => Math.max(m, n.x), 40);
  const newNode: PipelineNodeData = {
   id: newId,
   type: template.type,
   label: template.label,
   subtitle: template.subtitle,
   azureService: template.service,
   x: Math.min(1040, maxX + 145),
   y: 120,
   config: {}
  };
  const lastNode = nodes[nodes.length - 1];
  setNodes(prev => [...prev, newNode]);
  if (lastNode && NODE_STAGE_ORDER[lastNode.type] <= NODE_STAGE_ORDER[newNode.type]) {
   setEdges(prev => [...prev, { id: `e-${lastNode.id}-${newId}`, source: lastNode.id, target: newId }]);
  }
  setSelectedNodeId(newId);
 };

 const handleRemoveSelectedNode = () => {
  if (!selectedNodeId || nodes.length <= 2) return;
  setNodes(prev => prev.filter(n => n.id !== selectedNodeId));
  setEdges(prev => prev.filter(e => e.source !== selectedNodeId && e.target !== selectedNodeId));
  setSelectedNodeId(nodes[0]?.id || '');
 };

 const handleConnectClick = (targetId: string) => {
  if (!connectingFromId) {
   setConnectingFromId(targetId);
   return;
  }
  if (connectingFromId === targetId) {
   setConnectingFromId(null);
   return;
  }
  const srcNode = nodes.find(n => n.id === connectingFromId);
  const tgtNode = nodes.find(n => n.id === targetId);
  if (srcNode && tgtNode) {
   if (NODE_STAGE_ORDER[srcNode.type] > NODE_STAGE_ORDER[tgtNode.type]) {
    setValidationResult({
     valid: false,
     messages: [`Blocked invalid connection: ${srcNode.label} (${srcNode.type}) cannot connect upstream to ${tgtNode.label} (${tgtNode.type}).`]
    });
    setConnectingFromId(null);
    return;
   }
   const exists = edges.some(e => e.source === connectingFromId && e.target === targetId);
   if (!exists) {
    setEdges(prev => [...prev, { id: `e-${connectingFromId}-${targetId}`, source: connectingFromId, target: targetId }]);
   }
  }
  setConnectingFromId(null);
 };

 const handleSaveAndValidate = (runAfterSave: boolean) => {
  const check = validateCurrentGraph();
  setValidationResult(check);
  if (!check.valid) return;

  const updated: PipelineDefinition = {
   ...pipeline,
   name: pipelineName,
   nodes,
   edges,
   transformConfig,
   lastModified: new Date().toLocaleTimeString()
  };
  onSavePipeline(updated);
  if (runAfterSave) {
   onRunCustomPipeline(updated, false);
  }
 };

 const activeDiffStep = useMemo(
  () => livePreviewResult.steps.find(s => s.id === selectedStepDiffId) || livePreviewResult.steps[0],
  [livePreviewResult.steps, selectedStepDiffId]
 );

 return (
  <div className="space-y-6">
   {/* Top Header & Actions */}
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Visual Pipeline Builder & Transformation Studio</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Drag nodes, configure cleaning/filtering/aggregation rules, validate DAG topology, and inspect real-time before/after data diffs.
     </p>
    </div>

    <div className="flex flex-wrap items-center gap-2">
     <select
      value={pipeline.id}
      onChange={e => onSelectSavedPipeline(e.target.value)}
      className={`text-xs font-medium rounded-lg border px-3 py-2 ${darkMode ? 'bg-slate-800 border-slate-700 text-slate-100' : 'bg-white border-slate-200 text-slate-800'}`}
     >
      {savedPipelines.map(p => (
       <option key={p.id} value={p.id}>{p.name}</option>
      ))}
     </select>

     <button
      type="button"
      onClick={() => setValidationResult(validateCurrentGraph())}
      className={`inline-flex items-center gap-1.5 px-3 py-2 rounded-lg border text-xs font-medium cursor-pointer ${
       darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
      }`}
     >
      <ShieldCheck className="w-3.5 h-3.5 text-emerald-500" />
      Validate DAG
     </button>

     <button
      type="button"
      onClick={() => handleSaveAndValidate(false)}
      className={`inline-flex items-center gap-1.5 px-3 py-2 rounded-lg border text-xs font-medium cursor-pointer ${
       darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
      }`}
     >
      <Save className="w-3.5 h-3.5 text-[#0078D4]" />
      Save Pipeline
     </button>

     <button
      type="button"
      disabled={isExecuting}
      onClick={() => handleSaveAndValidate(true)}
      className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] hover:bg-blue-700 text-white shadow-sm cursor-pointer disabled:opacity-60"
     >
      {isExecuting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
      Run Validated Pipeline
     </button>
    </div>
   </div>

   {/* Validation Banner */}
   {validationResult && (
    <div className={`p-3.5 rounded-xl border text-xs flex items-center justify-between ${
     validationResult.valid
      ? 'bg-emerald-50 border-emerald-200 text-emerald-800 dark:bg-emerald-950/40 dark:border-emerald-800 dark:text-emerald-300'
      : 'bg-red-50 border-red-200 text-red-800 dark:bg-red-950/40 dark:border-red-800 dark:text-red-300'
    }`}>
     <div className="flex items-center gap-2">
      {validationResult.valid ? <CheckCircle2 className="w-4 h-4 text-emerald-500" /> : <AlertCircle className="w-4 h-4 text-red-500" />}
      <span>
       {validationResult.valid
        ? `Pipeline DAG verified (${nodes.length} nodes, ${edges.length} edges). Ready for execution.`
        : validationResult.messages.join(' | ')}
      </span>
     </div>
     <button type="button" onClick={() => setValidationResult(null)} className="text-xs underline cursor-pointer">
      Dismiss
     </button>
    </div>
   )}

   {/* Node Palette Bar */}
   <div className={`p-3 rounded-xl border flex flex-wrap items-center justify-between gap-2 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div className="flex flex-wrap items-center gap-1.5">
     <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400 mr-2">
      Add Node:
     </span>
     {NODE_PALETTE.map(item => (
      <button
       key={item.type}
       type="button"
       onClick={() => handleAddNode(item)}
       className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-lg border text-[11px] font-medium transition-all cursor-pointer ${
        darkMode
         ? 'bg-slate-800 border-slate-700 hover:border-blue-500 text-slate-200'
         : 'bg-slate-50 border-slate-200 hover:border-[#0078D4] text-slate-700'
       }`}
      >
       <Plus className="w-3 h-3 text-[#0078D4]" />
       {item.type}
      </button>
     ))}
    </div>

    {/* Canvas Zoom / Fit Controls */}
    <div className="flex items-center gap-1.5 text-xs">
     <button
      type="button"
      onClick={() => setZoom(z => Math.max(0.7, Number((z - 0.1).toFixed(2))))}
      className="px-2 py-1 rounded border border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer"
     >
      -
     </button>
     <span className="font-mono text-[11px] px-1 tabular-nums">{Math.round(zoom * 100)}%</span>
     <button
      type="button"
      onClick={() => setZoom(z => Math.min(1.3, Number((z + 0.1).toFixed(2))))}
      className="px-2 py-1 rounded border border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800 cursor-pointer"
     >
      +
     </button>
     <button
      type="button"
      onClick={() => { setZoom(1); setPan({ x: 0, y: 0 }); }}
      className="px-2.5 py-1 rounded border border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800 text-[11px] cursor-pointer"
     >
      Fit Screen
     </button>
     {selectedNodeId && nodes.length > 2 && (
      <button
       type="button"
       onClick={handleRemoveSelectedNode}
       className="px-2.5 py-1 rounded border border-red-200 dark:border-red-800 text-red-600 dark:text-red-400 text-[11px] hover:bg-red-50 cursor-pointer"
      >
       Delete Node
      </button>
     )}
    </div>
   </div>

   {/* Main Builder Grid: Canvas (8 cols) + Node & Rule Configuration Inspector (4 cols) */}
   <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
    {/* Interactive DAG Canvas */}
    <div
     ref={canvasRef}
     onMouseMove={handleCanvasMouseMove}
     onMouseUp={handleCanvasMouseUp}
     onMouseLeave={handleCanvasMouseUp}
     className={`lg:col-span-8 h-[420px] rounded-xl border relative overflow-auto select-none ${
      darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200 shadow-inner'
     }`}
     style={{
      backgroundImage: darkMode
       ? 'radial-gradient(#1e293b 1px, transparent 1px)'
       : 'radial-gradient(#cbd5e1 1px, transparent 1px)',
      backgroundSize: '20px 20px'
     }}
    >
     <div className="absolute top-3 left-3 z-20 flex items-center gap-2 pointer-events-none">
      <span className="px-2.5 py-1 rounded-md text-[11px] font-medium bg-white/90 dark:bg-slate-900/90 border border-slate-200 dark:border-slate-800 shadow-sm">
       Drag any node to move • Click port icon to connect compatible stages
      </span>
      {connectingFromId && (
       <span className="px-2.5 py-1 rounded-md text-[11px] font-semibold bg-[#0078D4] text-white shadow-sm">
        Select target node to complete connection...
       </span>
      )}
     </div>

     <div
      className="relative min-w-[1220px] h-[400px] origin-top-left transition-transform duration-75"
      style={{ transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})` }}
     >
      {/* SVG Edges */}
      <svg className="absolute inset-0 w-full h-full pointer-events-none">
       <defs>
        <marker id="arrowHead" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
         <path d="M0,0 L8,4 L0,8 Z" fill="#0078D4" />
        </marker>
       </defs>
       {edges.map(edge => {
        const s = nodes.find(n => n.id === edge.source);
        const t = nodes.find(n => n.id === edge.target);
        if (!s || !t) return null;
        const x1 = s.x + 148;
        const y1 = s.y + 42;
        const x2 = t.x;
        const y2 = t.y + 42;
        const cx1 = x1 + (x2 - x1) * 0.5;
        const cx2 = x1 + (x2 - x1) * 0.5;
        const dPath = `M ${x1} ${y1} C ${cx1} ${y1}, ${cx2} ${y2}, ${x2} ${y2}`;
        return (
         <g key={edge.id}>
          <path
           d={dPath}
           fill="none"
           stroke={darkMode ? '#3b82f6' : '#0078D4'}
           strokeWidth={2.2}
           markerEnd="url(#arrowHead)"
          />
         </g>
        );
       })}
      </svg>

      {/* Nodes */}
      {nodes.map(node => {
       const isSelected = node.id === selectedNodeId;
       const isConnectSrc = node.id === connectingFromId;
       return (
        <div
         key={node.id}
         onMouseDown={e => handleNodeMouseDown(e, node)}
         style={{ left: `${node.x}px`, top: `${node.y}px`, width: '148px' }}
         className={`absolute rounded-xl border p-3 cursor-grab active:cursor-grabbing transition-shadow ${
          isSelected
           ? 'ring-2 ring-[#0078D4] border-[#0078D4] shadow-lg'
           : darkMode
           ? 'border-slate-700 hover:border-slate-500 shadow-md'
           : 'border-slate-200 hover:border-blue-300 shadow-sm'
         } ${darkMode ? 'bg-slate-900 text-slate-100' : 'bg-white text-slate-900'}`}
        >
         <div className="flex items-center justify-between mb-1">
          <span className="text-[9px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded bg-[#0078D4]/10 text-[#0078D4] dark:text-blue-400">
           {node.type}
          </span>
          <button
           type="button"
           title="Connect to downstream node"
           onMouseDown={e => e.stopPropagation()}
           onClick={e => {
            e.stopPropagation();
            handleConnectClick(node.id);
           }}
           className={`p-1 rounded hover:bg-blue-500/10 cursor-pointer ${
            isConnectSrc ? 'text-white bg-[#0078D4]' : 'text-slate-400 hover:text-[#0078D4]'
           }`}
          >
           <Link2 className="w-3 h-3" />
          </button>
         </div>
         <div className="font-semibold text-xs truncate">{node.label}</div>
         <div className="text-[10px] text-slate-400 truncate mt-0.5">{node.azureService}</div>
        </div>
       );
      })}
     </div>
    </div>

    {/* Right Side: Node & Transformation Engine Configuration */}
    <div className={`lg:col-span-4 rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="flex items-center justify-between pb-3 border-b border-slate-100 dark:border-slate-800">
      <div>
       <h3 className="font-semibold text-sm">
        {selectedNode ? `Configure: ${selectedNode.label}` : 'Transformation Rules'}
       </h3>
       <p className="text-[11px] text-slate-400">
        Changes recalculate the live preview below immediately
       </p>
      </div>
      <Wrench className="w-4 h-4 text-[#0078D4]" />
     </div>

     <div className="space-y-3 text-xs">
      <div>
       <label className="block font-medium mb-1">Pipeline Name</label>
       <input
        type="text"
        value={pipelineName}
        onChange={e => setPipelineName(e.target.value)}
        className={`w-full px-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>

      <div className="pt-1 space-y-2">
       <div className="font-semibold text-slate-400 uppercase text-[10px] tracking-wider">
        Cleaning & Validation Rules
       </div>

       <label className="flex items-center justify-between py-1 cursor-pointer">
        <span>Remove Exact Duplicate Records</span>
        <input
         type="checkbox"
         checked={transformConfig.deduplicateExact}
         onChange={e => setTransformConfig({ ...transformConfig, deduplicateExact: e.target.checked })}
        />
       </label>

       <div className="flex items-center justify-between py-1">
        <span>Deduplicate Business Key</span>
        <select
         value={transformConfig.deduplicateBusinessKey}
         onChange={e => setTransformConfig({ ...transformConfig, deduplicateBusinessKey: e.target.value })}
         className={`px-2 py-1 rounded border text-xs ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        >
         <option value="order_id">order_id (Primary Key)</option>
         <option value="none">Disabled</option>
        </select>
       </div>

       <label className="flex items-center justify-between py-1 cursor-pointer">
        <span>Normalize Whitespace & Trim Strings</span>
        <input
         type="checkbox"
         checked={transformConfig.normalizeWhitespace}
         onChange={e => setTransformConfig({ ...transformConfig, normalizeWhitespace: e.target.checked })}
        />
       </label>

       <label className="flex items-center justify-between py-1 cursor-pointer">
        <span>Standardize Category Labels (Title Case)</span>
        <input
         type="checkbox"
         checked={transformConfig.standardizeCategories}
         onChange={e => setTransformConfig({ ...transformConfig, standardizeCategories: e.target.checked })}
        />
       </label>

       <label className="flex items-center justify-between py-1 cursor-pointer">
        <span>Impute Missing Fields (Median / Default)</span>
        <input
         type="checkbox"
         checked={transformConfig.imputeMissingValues}
         onChange={e => setTransformConfig({ ...transformConfig, imputeMissingValues: e.target.checked })}
        />
       </label>

       <label className="flex items-center justify-between py-1 cursor-pointer">
        <span>Quarantine Unparseable ISO Dates</span>
        <input
         type="checkbox"
         checked={transformConfig.dropInvalidDates}
         onChange={e => setTransformConfig({ ...transformConfig, dropInvalidDates: e.target.checked })}
        />
       </label>

       <label className="flex items-center justify-between py-1 cursor-pointer">
        <span>Fix Negative Quantity & Price Signs</span>
        <input
         type="checkbox"
         checked={transformConfig.fixNegativeNumbers}
         onChange={e => setTransformConfig({ ...transformConfig, fixNegativeNumbers: e.target.checked })}
        />
       </label>
      </div>

      {/* Conditional Filtering Section */}
      <div className="pt-2 border-t border-slate-100 dark:border-slate-800 space-y-2">
       <label className="flex items-center justify-between font-semibold cursor-pointer">
        <span className="text-[#0078D4] dark:text-blue-400">Enable Row Filter Rule</span>
        <input
         type="checkbox"
         checked={transformConfig.filterEnabled}
         onChange={e => setTransformConfig({ ...transformConfig, filterEnabled: e.target.checked })}
        />
       </label>

       {transformConfig.filterEnabled && (
        <div className="grid grid-cols-3 gap-1.5">
         <select
          value={transformConfig.filterColumn}
          onChange={e => setTransformConfig({ ...transformConfig, filterColumn: e.target.value })}
          className={`px-2 py-1.5 rounded border text-[11px] ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
         >
          <option value="revenue">revenue</option>
          <option value="quantity">quantity</option>
          <option value="category">category</option>
          <option value="region">region</option>
          <option value="product_name">product_name</option>
         </select>
         <select
          value={transformConfig.filterOperator}
          onChange={e => setTransformConfig({ ...transformConfig, filterOperator: e.target.value as any })}
          className={`px-2 py-1.5 rounded border text-[11px] ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
         >
          <option value="gt">&gt; Greater</option>
          <option value="lt">&lt; Less</option>
          <option value="equals">= Equals</option>
          <option value="contains">Contains</option>
          <option value="not_null">Not Null</option>
         </select>
         <input
          type="text"
          placeholder="Value..."
          value={transformConfig.filterValue}
          onChange={e => setTransformConfig({ ...transformConfig, filterValue: e.target.value })}
          className={`px-2 py-1.5 rounded border text-[11px] ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
         />
        </div>
       )}
      </div>

      {/* Aggregation Config */}
      <div className="pt-2 border-t border-slate-100 dark:border-slate-800 grid grid-cols-2 gap-2">
       <div>
        <label className="block text-[11px] text-slate-400 mb-1">Aggregate Group By</label>
        <select
         value={transformConfig.aggregateGroupBy}
         onChange={e => setTransformConfig({ ...transformConfig, aggregateGroupBy: e.target.value })}
         className={`w-full px-2 py-1.5 rounded border text-xs ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        >
         <option value="category">category</option>
         <option value="region">region</option>
         <option value="customer_segment">customer_segment</option>
         <option value="order_month">order_month</option>
        </select>
       </div>
       <div>
        <label className="block text-[11px] text-slate-400 mb-1">Primary Metric</label>
        <select
         value={transformConfig.aggregateMetric}
         onChange={e => setTransformConfig({ ...transformConfig, aggregateMetric: e.target.value as any })}
         className={`w-full px-2 py-1.5 rounded border text-xs ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        >
         <option value="sum">SUM(revenue)</option>
         <option value="mean">MEAN(revenue)</option>
         <option value="count">COUNT(orders)</option>
         <option value="max">MAX(revenue)</option>
        </select>
       </div>
      </div>
     </div>
    </div>
   </div>

   {/* Modular Transformation Steps & Before/After Preview Inspector */}
   <div className={`rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
     <div>
      <h3 className="font-semibold text-sm">Transformation Step Execution & Before/After Inspector</h3>
      <p className="text-xs text-slate-500 dark:text-slate-400">
       Select any transformation step below to compare input vs. transformed records ({activeDataset.rawRows.length} raw → {livePreviewResult.curatedRows.length} curated rows).
      </p>
     </div>
     <span className="text-xs font-mono px-2.5 py-1 rounded bg-emerald-500/10 text-emerald-600 dark:text-emerald-400">
      Quarantined/Removed: {livePreviewResult.quarantinedRows.length} rows
     </span>
    </div>

    <div className="grid grid-cols-1 md:grid-cols-5 gap-3 mb-5">
     {livePreviewResult.steps.map(step => {
      const isSel = step.id === activeDiffStep?.id;
      return (
       <button
        key={step.id}
        type="button"
        onClick={() => setSelectedStepDiffId(step.id)}
        className={`text-left p-3 rounded-xl border transition-all cursor-pointer ${
         isSel
          ? 'border-[#0078D4] bg-blue-50/50 dark:bg-blue-950/40 ring-2 ring-[#0078D4]/20'
          : darkMode
          ? 'border-slate-800 bg-slate-950/50 hover:border-slate-700'
          : 'border-slate-200 bg-slate-50/70 hover:border-slate-300'
        }`}
       >
        <div className="flex items-center justify-between text-[10px] font-bold uppercase text-[#0078D4] dark:text-blue-400 mb-1">
         <span>{step.type}</span>
         <span>{step.durationMs}ms</span>
        </div>
        <div className="font-semibold text-xs line-clamp-1">{step.name}</div>
        <div className="text-[11px] text-slate-500 mt-1.5 tabular-nums">
         {step.inputRows} → <strong>{step.outputRows}</strong> rows
         {step.removedRows > 0 && <span className="text-red-500 ml-1">(-{step.removedRows})</span>}
         {step.modifiedRows > 0 && <span className="text-emerald-600 ml-1">({step.modifiedRows} mod)</span>}
        </div>
       </button>
      );
     })}
    </div>

    {activeDiffStep && (
     <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 text-xs">
      <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-950/60 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
       <div className="font-semibold text-amber-600 dark:text-amber-400 mb-2">
        BEFORE: {activeDiffStep.name} (Sample Input)
       </div>
       <div className="overflow-x-auto">
        <table className="w-full text-left text-[11px] font-mono">
         <thead>
          <tr className="border-b border-slate-200 dark:border-slate-800 text-slate-400">
           <th className="py-1.5 px-2">order_id</th>
           <th className="py-1.5 px-2">order_date</th>
           <th className="py-1.5 px-2">category</th>
           <th className="py-1.5 px-2 text-right">qty</th>
           <th className="py-1.5 px-2 text-right">price</th>
          </tr>
         </thead>
         <tbody className="divide-y divide-slate-200/50 dark:divide-slate-800">
          {activeDiffStep.sampleBefore.map((r, i) => (
           <tr key={i}>
            <td className="py-1.5 px-2">{r.order_id}</td>
            <td className="py-1.5 px-2">{r.order_date}</td>
            <td className="py-1.5 px-2">"{String(r.category)}"</td>
            <td className="py-1.5 px-2 text-right">{String(r.quantity)}</td>
            <td className="py-1.5 px-2 text-right">{String(r.unit_price ?? 'NULL')}</td>
           </tr>
          ))}
         </tbody>
        </table>
       </div>
      </div>

      <div className={`p-3.5 rounded-xl border ${darkMode ? 'bg-slate-950/60 border-slate-800' : 'bg-emerald-50/30 border-emerald-200/80'}`}>
       <div className="font-semibold text-emerald-600 dark:text-emerald-400 mb-2">
        AFTER: {activeDiffStep.name} (Transformed Output)
       </div>
       <div className="overflow-x-auto">
        <table className="w-full text-left text-[11px] font-mono">
         <thead>
          <tr className="border-b border-slate-200 dark:border-slate-800 text-slate-400">
           <th className="py-1.5 px-2">order_id</th>
           <th className="py-1.5 px-2">order_date</th>
           <th className="py-1.5 px-2">category</th>
           <th className="py-1.5 px-2 text-right">qty</th>
           <th className="py-1.5 px-2 text-right">revenue/price</th>
          </tr>
         </thead>
         <tbody className="divide-y divide-slate-200/50 dark:divide-slate-800">
          {activeDiffStep.sampleAfter.map((r, i) => (
           <tr key={i}>
            <td className="py-1.5 px-2">{r.order_id}</td>
            <td className="py-1.5 px-2">{r.order_date}</td>
            <td className="py-1.5 px-2 text-emerald-600 dark:text-emerald-400 font-semibold">
             "{String(r.category)}"
            </td>
            <td className="py-1.5 px-2 text-right">{String(r.quantity)}</td>
            <td className="py-1.5 px-2 text-right font-semibold">
             ${Number(r.revenue ?? r.unit_price ?? 0).toLocaleString()}
            </td>
           </tr>
          ))}
         </tbody>
        </table>
       </div>
      </div>
     </div>
    )}
   </div>
  </div>
 );
};

// ============================================================================
// 10. PIPELINE MONITOR VIEW
// ============================================================================

const PipelineMonitorView: React.FC<{
 executions: PipelineExecutionRecord[];
 isExecuting: boolean;
 onRunPipeline: (simulateFailure?: boolean) => void;
 onRetryExecution: (executionId: string) => void;
 darkMode: boolean;
}> = ({ executions, isExecuting, onRunPipeline, onRetryExecution, darkMode }) => {
 const [statusFilter, setStatusFilter] = useState<string>('ALL');
 const [triggerFilter, setTriggerFilter] = useState<string>('ALL');
 const [selectedExecId, setSelectedExecId] = useState<string>(executions[0]?.executionId || '');

 const filteredExecutions = useMemo(() => {
  return executions.filter(ex => {
   if (statusFilter !== 'ALL' && ex.status !== statusFilter) return false;
   if (triggerFilter !== 'ALL' && ex.triggerType !== triggerFilter) return false;
   return true;
  });
 }, [executions, statusFilter, triggerFilter]);

 const selectedExec = useMemo(
  () => executions.find(e => e.executionId === selectedExecId) || filteredExecutions[0] || executions[0],
  [executions, selectedExecId, filteredExecutions]
 );

 const runningCount = executions.filter(e => e.status === 'Running').length;
 const queuedCount = executions.filter(e => e.status === 'Queued').length;
 const successCount = executions.filter(e => e.status === 'Succeeded').length;
 const failedCount = executions.filter(e => e.status === 'Failed').length;
 const avgDuration = executions.length > 0
  ? Math.round(executions.reduce((a, e) => a + e.totalDurationMs, 0) / executions.length)
  : 0;

 return (
  <div className="space-y-6">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Pipeline Execution Monitor</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Track real-time execution states, inspect stage-level telemetry, and retry failed runs without duplicating output records.
     </p>
    </div>
    <div className="flex items-center gap-2">
     <button
      type="button"
      disabled={isExecuting}
      onClick={() => onRunPipeline(true)}
      className="px-3 py-2 rounded-lg border border-red-200 dark:border-red-800 text-red-600 dark:text-red-400 text-xs font-medium hover:bg-red-50 dark:hover:bg-red-950/40 cursor-pointer"
     >
      Simulate Schema Fault
     </button>
     <button
      type="button"
      disabled={isExecuting}
      onClick={() => onRunPipeline(false)}
      className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] hover:bg-blue-700 text-white cursor-pointer"
     >
      <Play className="w-3.5 h-3.5" />
      Trigger Pipeline Run
     </button>
    </div>
   </div>

   {/* Overview KPIs */}
   <div className="grid grid-cols-2 sm:grid-cols-5 gap-3.5">
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Running Pipelines</div>
     <div className="text-2xl font-bold text-[#0078D4] tabular-nums mt-1">{runningCount}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Queued Jobs</div>
     <div className="text-2xl font-bold tabular-nums mt-1">{queuedCount}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Successful Runs</div>
     <div className="text-2xl font-bold text-emerald-500 tabular-nums mt-1">{successCount}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Failed Runs</div>
     <div className="text-2xl font-bold text-red-500 tabular-nums mt-1">{failedCount}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Avg Execution Time</div>
     <div className="text-2xl font-bold tabular-nums mt-1">{avgDuration} ms</div>
    </div>
   </div>

   {/* Filters */}
   <div className={`p-3.5 rounded-xl border flex flex-wrap items-center gap-3 text-xs ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200'}`}>
    <div className="flex items-center gap-2">
     <Filter className="w-3.5 h-3.5 text-slate-400" />
     <span className="font-medium">Status:</span>
     {['ALL', 'Succeeded', 'Failed', 'Running'].map(s => (
      <button
       key={s}
       type="button"
       onClick={() => setStatusFilter(s)}
       className={`px-2.5 py-1 rounded-md font-medium cursor-pointer ${
        statusFilter === s ? 'bg-[#0078D4] text-white' : 'hover:bg-slate-100 dark:hover:bg-slate-800'
       }`}
      >
       {s}
      </button>
     ))}
    </div>

    <div className="flex items-center gap-2 ml-auto">
     <span className="font-medium">Trigger:</span>
     <select
      value={triggerFilter}
      onChange={e => setTriggerFilter(e.target.value)}
      className={`px-2.5 py-1 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="ALL">All Triggers</option>
      <option value="Manual">Manual</option>
      <option value="File Upload">File Upload</option>
      <option value="Scheduled ADF Trigger">Scheduled ADF Trigger</option>
      <option value="Auto-Retry">Auto-Retry</option>
     </select>
    </div>
   </div>

   {/* Master-Detail Split */}
   <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
    {/* Execution History List */}
    <div className={`lg:col-span-5 rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="px-4 py-3 border-b border-slate-100 dark:border-slate-800 font-semibold text-xs">
      Execution History ({filteredExecutions.length} runs)
     </div>
     <div className="divide-y divide-slate-100 dark:divide-slate-800 max-h-[500px] overflow-y-auto">
      {filteredExecutions.map(ex => {
       const isSel = ex.executionId === selectedExec?.executionId;
       return (
        <div
         key={ex.executionId}
         onClick={() => setSelectedExecId(ex.executionId)}
         className={`p-4 cursor-pointer transition-colors ${
          isSel
           ? 'bg-blue-50/70 dark:bg-blue-950/30 border-l-4 border-l-[#0078D4]'
           : 'hover:bg-slate-50 dark:hover:bg-slate-800/40'
         }`}
        >
         <div className="flex items-center justify-between mb-1">
          <span className="font-mono font-semibold text-xs text-[#0078D4] dark:text-blue-400">
           #{ex.executionId}
          </span>
          <StatusBadge status={ex.status} />
         </div>
         <div className="font-semibold text-xs">{ex.pipelineName}</div>
         <div className="text-[11px] text-slate-500 mt-0.5">
          Dataset: {ex.datasetName} • Trigger: {ex.triggerType}
         </div>
         <div className="mt-2 flex items-center justify-between text-[11px] text-slate-400 tabular-nums">
          <span>{ex.startTime}</span>
          <span>{ex.inputRowCount} → {ex.outputRowCount} rows ({ex.totalDurationMs}ms)</span>
         </div>
        </div>
       );
      })}
     </div>
    </div>

    {/* Execution Detail Inspector */}
    {selectedExec && (
     <div className={`lg:col-span-7 rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
      <div className="flex flex-wrap items-center justify-between gap-2 pb-3 border-b border-slate-100 dark:border-slate-800">
       <div>
        <div className="flex items-center gap-2">
         <h3 className="font-bold text-base">Execution #{selectedExec.executionId}</h3>
         <StatusBadge status={selectedExec.status} />
         {selectedExec.retryCount > 0 && (
          <span className="text-xs px-2 py-0.5 rounded bg-indigo-500/10 text-indigo-500 font-mono">
           Retries: {selectedExec.retryCount}
          </span>
         )}
        </div>
        <p className="text-xs text-slate-500 mt-0.5">
         Pipeline: <strong>{selectedExec.pipelineName}</strong> • Dataset: <strong>{selectedExec.datasetName}</strong>
        </p>
       </div>

       {selectedExec.status === 'Failed' && (
        <button
         type="button"
         onClick={() => onRetryExecution(selectedExec.executionId)}
         className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg text-xs font-semibold bg-amber-500 hover:bg-amber-600 text-white shadow-sm cursor-pointer"
        >
         <RotateCcw className="w-3.5 h-3.5" />
         Retry Failed Execution
        </button>
       )}
      </div>

      {selectedExec.errorMessage && (
       <div className="p-3.5 rounded-xl bg-red-500/10 border border-red-500/30 text-red-600 dark:text-red-300 text-xs">
        <div className="font-bold mb-0.5">Mandatory Stage Failure — Dependent Downstream Stages Halted:</div>
        <div>{selectedExec.errorMessage}</div>
       </div>
      )}

      {/* Stage-Level Breakdown */}
      <div>
       <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2.5">
        Stage-Level Execution Breakdown
       </h4>
       <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
        <table className="w-full text-left text-xs">
         <thead className={darkMode ? 'bg-slate-950 text-slate-400' : 'bg-slate-50 text-slate-500'}>
          <tr>
           <th className="py-2 px-3">Stage</th>
           <th className="py-2 px-3">Azure Service</th>
           <th className="py-2 px-3 text-right">In Rows</th>
           <th className="py-2 px-3 text-right">Out Rows</th>
           <th className="py-2 px-3 text-right">Duration</th>
           <th className="py-2 px-3">Status</th>
          </tr>
         </thead>
         <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
          {selectedExec.stages.map(stg => (
           <tr key={stg.stageId}>
            <td className="py-2 px-3 font-medium">{stg.stageName}</td>
            <td className="py-2 px-3 text-slate-500">{stg.azureService}</td>
            <td className="py-2 px-3 text-right">{stg.inputRows}</td>
            <td className="py-2 px-3 text-right font-semibold">{stg.outputRows}</td>
            <td className="py-2 px-3 text-right font-mono">{stg.durationMs} ms</td>
            <td className="py-2 px-3">
             <StatusBadge status={stg.status} />
            </td>
           </tr>
          ))}
         </tbody>
        </table>
       </div>
      </div>

      {/* Execution Logs */}
      <div>
       <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
        Stage Execution Logs
       </h4>
       <div className="p-3.5 rounded-xl bg-slate-950 text-slate-200 font-mono text-[11px] space-y-1 max-h-48 overflow-y-auto">
        {selectedExec.stages.flatMap(stg =>
         stg.logs.map((l, i) => (
          <div key={`${stg.stageId}-${i}`}>
           <span className="text-blue-400">[{stg.stageName}]</span> {l}
          </div>
         ))
        )}
       </div>
      </div>
     </div>
    )}
   </div>
  </div>
 );
};

// ============================================================================
// 11. DATA QUALITY ENGINE VIEW
// ============================================================================

const DataQualityView: React.FC<{
 dataset: DatasetEntity;
 onRemediateAndRun: () => void;
 darkMode: boolean;
}> = ({ dataset, onRemediateAndRun, darkMode }) => {
 const [layer, setLayer] = useState<'raw' | 'curated'>('raw');
 const [threshold, setThreshold] = useState<number>(95);
 const [selectedRuleId, setSelectedRuleId] = useState<string>('chk-category-consistency');

 const report = layer === 'raw' ? dataset.rawQuality : dataset.curatedQuality;
 const selectedRule = useMemo(
  () => report.checks.find(c => c.id === selectedRuleId) || report.checks[0],
  [report, selectedRuleId]
 );

 const handleExportQualityReport = () => {
  const payload = {
   datasetName: dataset.name,
   evaluatedLayer: layer,
   generatedAt: new Date().toISOString(),
   scoringFormula: '0.30 * Completeness + 0.25 * Validity + 0.25 * Uniqueness + 0.20 * Consistency',
   qualityReport: report
  };
  triggerFileDownload(JSON.stringify(payload, null, 2), `${dataset.id}_quality_report_${layer}.json`, 'application/json');
 };

 return (
  <div className="space-y-6">
   {/* Header */}
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Data Quality Engine & Rule Inspector</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Transparent quality scoring formula, column-level null profiling, and interactive inspection of rows failing validation rules.
     </p>
    </div>

    <div className="flex items-center gap-2">
     <div className="inline-flex rounded-xl p-1 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700">
      <button
       type="button"
       onClick={() => setLayer('raw')}
       className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all cursor-pointer ${
        layer === 'raw' ? 'bg-white dark:bg-slate-900 text-amber-600 shadow-sm' : 'text-slate-500'
       }`}
      >
       Raw Bronze ({dataset.rawQuality.overallScore}%)
      </button>
      <button
       type="button"
       onClick={() => setLayer('curated')}
       className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all cursor-pointer ${
        layer === 'curated' ? 'bg-white dark:bg-slate-900 text-emerald-600 shadow-sm' : 'text-slate-500'
       }`}
      >
       Curated Gold ({dataset.curatedQuality.overallScore}%)
      </button>
     </div>

     <button
      type="button"
      onClick={handleExportQualityReport}
      className={`inline-flex items-center gap-1.5 px-3 py-2 rounded-lg border text-xs font-medium cursor-pointer ${
       darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
      }`}
     >
      <Download className="w-3.5 h-3.5 text-[#0078D4]" />
      Export Quality Report
     </button>
    </div>
   </div>

   {/* Transparent Scoring Formula Banner */}
   <div className={`rounded-xl border p-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-blue-50/50 border-blue-200/80'}`}>
    <div className="flex flex-wrap items-center justify-between gap-4">
     <div>
      <div className="text-xs font-bold uppercase tracking-wider text-[#0078D4] dark:text-blue-400">
       Documented Quality Scoring Formula
      </div>
      <div className="text-xs font-mono mt-1 text-slate-700 dark:text-slate-200">
       Quality Score ({report.overallScore}%) = 0.30 × Completeness ({report.completenessScore}%) + 0.25 × Validity ({report.validityScore}%) + 0.25 × Uniqueness ({report.uniquenessScore}%) + 0.20 × Consistency ({report.consistencyScore}%)
      </div>
     </div>

     <div className="flex items-center gap-3 text-xs">
      <span className="font-medium">SLA Threshold: <strong>{threshold}%</strong></span>
      <input
       type="range"
       min={75}
       max={100}
       value={threshold}
       onChange={e => setThreshold(Number(e.target.value))}
       className="w-28 accent-[#0078D4]"
      />
      <StatusBadge status={report.overallScore >= threshold ? 'SLA Passed' : 'Below SLA Threshold'} />
     </div>
    </div>
   </div>

   {/* 5 Metric Cards */}
   <div className="grid grid-cols-2 sm:grid-cols-5 gap-3.5">
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Total Evaluated Rows</div>
     <div className="text-2xl font-bold tabular-nums mt-1">{report.totalRows}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">100% Clean Rows</div>
     <div className="text-2xl font-bold text-emerald-500 tabular-nums mt-1">{report.validRows}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Invalid Date/Numeric</div>
     <div className="text-2xl font-bold text-red-500 tabular-nums mt-1">{report.invalidRows}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Duplicate Key Rows</div>
     <div className="text-2xl font-bold text-amber-500 tabular-nums mt-1">{report.duplicateRows}</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Inconsistent Category</div>
     <div className="text-2xl font-bold text-indigo-500 tabular-nums mt-1">{report.inconsistentRows}</div>
    </div>
   </div>

   {/* Null Counts By Column */}
   <div className={`rounded-xl border p-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <h3 className="font-semibold text-xs uppercase tracking-wider text-slate-400 mb-3">
     Null / Missing Value Counts by Required Column ({layer.toUpperCase()} Layer)
    </h3>
    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-2.5 text-xs">
     {Object.entries(report.nullCountsByColumn).map(([col, cnt]) => (
      <div key={col} className={`p-2.5 rounded-lg border ${cnt > 0 ? 'border-amber-300 bg-amber-50/50 dark:border-amber-800 dark:bg-amber-950/30' : darkMode ? 'border-slate-800 bg-slate-950/40' : 'border-slate-100 bg-slate-50'}`}>
       <div className="font-mono text-[11px] truncate text-slate-500">{col}</div>
       <div className={`text-base font-bold tabular-nums mt-0.5 ${cnt > 0 ? 'text-amber-600 dark:text-amber-400' : 'text-emerald-600 dark:text-emerald-400'}`}>
        {cnt} nulls
       </div>
      </div>
     ))}
    </div>
   </div>

   {/* Rules Table + Affected Rows Inspector */}
   <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
    <div className={`lg:col-span-6 rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="px-4 py-3 border-b border-slate-100 dark:border-slate-800 font-semibold text-xs">
      Validation Rules (Click any rule to inspect affected records)
     </div>
     <div className="divide-y divide-slate-100 dark:divide-slate-800">
      {report.checks.map(chk => {
       const isSel = chk.id === selectedRule?.id;
       return (
        <div
         key={chk.id}
         onClick={() => setSelectedRuleId(chk.id)}
         className={`p-3.5 cursor-pointer transition-colors ${
          isSel ? 'bg-blue-50/70 dark:bg-blue-950/30 border-l-4 border-l-[#0078D4]' : 'hover:bg-slate-50 dark:hover:bg-slate-800/40'
         }`}
        >
         <div className="flex items-center justify-between">
          <span className="font-semibold text-xs">{chk.name}</span>
          <StatusBadge status={`${chk.status} (${chk.passRate}%)`} />
         </div>
         <p className="text-[11px] text-slate-500 mt-1">{chk.description}</p>
         <div className="mt-2 flex items-center justify-between text-[11px] tabular-nums text-slate-400">
          <span>Dimension: <strong>{chk.dimension}</strong></span>
          <span>Failed: <strong className={chk.failedCount > 0 ? 'text-red-500' : 'text-emerald-500'}>{chk.failedCount}</strong> / {chk.totalCount}</span>
         </div>
        </div>
       );
      })}
     </div>
    </div>

    {/* Affected Rows Inspector */}
    {selectedRule && (
     <div className={`lg:col-span-6 rounded-xl border p-5 flex flex-col justify-between ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
      <div>
       <div className="flex items-center justify-between pb-3 border-b border-slate-100 dark:border-slate-800">
        <div>
         <h3 className="font-bold text-sm">{selectedRule.name}</h3>
         <p className="text-xs text-slate-500 mt-0.5">
          {selectedRule.affectedRows.length} affected rows in {layer.toUpperCase()} dataset
         </p>
        </div>
        {layer === 'raw' && selectedRule.failedCount > 0 && (
         <button
          type="button"
          onClick={() => {
           onRemediateAndRun();
           setLayer('curated');
          }}
          className="px-3 py-1.5 rounded-lg bg-[#0078D4] text-white text-xs font-semibold hover:bg-blue-700 cursor-pointer"
         >
          Remediate & Switch to Curated
         </button>
        )}
       </div>

       <div className="my-3 p-3 rounded-lg bg-indigo-500/10 border border-indigo-500/20 text-xs">
        <strong>Recommended Remediation:</strong> {selectedRule.remediation}
       </div>

       {selectedRule.affectedRows.length === 0 ? (
        <div className="py-12 text-center text-xs text-slate-500">
         <CheckCircle2 className="w-8 h-8 text-emerald-500 mx-auto mb-2" />
         Zero records violated <strong>{selectedRule.name}</strong> in this layer.
        </div>
       ) : (
        <div className="overflow-x-auto max-h-72 rounded-lg border border-slate-200 dark:border-slate-800">
         <table className="w-full text-left text-[11px] font-mono">
          <thead className={darkMode ? 'bg-slate-950 text-slate-400' : 'bg-slate-50 text-slate-500'}>
           <tr>
            <th className="py-2 px-2.5">order_id</th>
            <th className="py-2 px-2.5">order_date</th>
            <th className="py-2 px-2.5">category</th>
            <th className="py-2 px-2.5 text-right">qty</th>
            <th className="py-2 px-2.5 text-right">price</th>
            <th className="py-2 px-2.5">segment</th>
           </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
           {selectedRule.affectedRows.map((r, i) => (
            <tr key={i} className="bg-amber-500/5">
             <td className="py-1.5 px-2.5 font-semibold">{r.order_id}</td>
             <td className="py-1.5 px-2.5">{String(r.order_date)}</td>
             <td className="py-1.5 px-2.5">"{String(r.category)}"</td>
             <td className="py-1.5 px-2.5 text-right">{String(r.quantity)}</td>
             <td className="py-1.5 px-2.5 text-right">{String(r.unit_price ?? 'NULL')}</td>
             <td className="py-1.5 px-2.5">{String(r.customer_segment ?? 'NULL')}</td>
            </tr>
           ))}
          </tbody>
         </table>
        </div>
       )}
      </div>
     </div>
    )}
   </div>
  </div>
 );
};

// ============================================================================
// 12. DATASET EXPLORER VIEW
// ============================================================================

const DatasetExplorerView: React.FC<{
 datasets: DatasetEntity[];
 activeDatasetId: string;
 onSelectDataset: (id: string) => void;
 darkMode: boolean;
}> = ({ datasets, activeDatasetId, onSelectDataset, darkMode }) => {
 const dataset = useMemo(
  () => datasets.find(d => d.id === activeDatasetId) || datasets[0],
  [datasets, activeDatasetId]
 );

 const [viewMode, setViewMode] = useState<'curated' | 'raw' | 'quarantined'>('curated');
 const [activeTab, setActiveTab] = useState<'grid' | 'schema'>('grid');
 const [searchQuery, setSearchQuery] = useState('');
 const [categoryFilter, setCategoryFilter] = useState('ALL');
 const [sortCol, setSortCol] = useState<string>('order_id');
 const [sortAsc, setSortAsc] = useState<boolean>(true);
 const [page, setPage] = useState<number>(1);
 const [pageSize, setPageSize] = useState<number>(12);

 const activeRows = useMemo(() => {
  if (viewMode === 'raw') return dataset.rawRows;
  if (viewMode === 'quarantined') return dataset.quarantinedRows;
  return dataset.curatedRows;
 }, [dataset, viewMode]);

 const categories = useMemo(() => {
  const set = new Set<string>();
  activeRows.forEach(r => {
   if (r.category) set.add(String(r.category).trim());
  });
  return Array.from(set);
 }, [activeRows]);

 const filteredAndSortedRows = useMemo(() => {
  const q = searchQuery.trim().toLowerCase();
  const filtered = activeRows.filter(r => {
   if (categoryFilter !== 'ALL' && String(r.category || '').trim() !== categoryFilter) return false;
   if (!q) return true;
   return Object.values(r).some(v => String(v ?? '').toLowerCase().includes(q));
  });

  return [...filtered].sort((a, b) => {
   const va = a[sortCol];
   const vb = b[sortCol];
   if (typeof va === 'number' && typeof vb === 'number') {
    return sortAsc ? va - vb : vb - va;
   }
   const sa = String(va ?? '');
   const sb = String(vb ?? '');
   return sortAsc ? sa.localeCompare(sb) : sb.localeCompare(sa);
  });
 }, [activeRows, searchQuery, categoryFilter, sortCol, sortAsc]);

 const totalPages = Math.max(1, Math.ceil(filteredAndSortedRows.length / pageSize));
 const currentPage = Math.min(page, totalPages);
 const paginatedRows = useMemo(
  () => filteredAndSortedRows.slice((currentPage - 1) * pageSize, currentPage * pageSize),
  [filteredAndSortedRows, currentPage, pageSize]
 );

 const handleSort = (col: string) => {
  if (sortCol === col) {
   setSortAsc(!sortAsc);
  } else {
   setSortCol(col);
   setSortAsc(true);
  }
 };

 const activeSchema = viewMode === 'raw' ? dataset.schema : dataset.curatedSchema;

 return (
  <div className="space-y-6">
   {/* Header */}
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Dataset Explorer & Schema Profiler</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Browse Raw Bronze, Curated Gold, and Quarantined datasets with sorting, filtering, pagination, and column statistics.
     </p>
    </div>

    <div className="flex flex-wrap items-center gap-2">
     <select
      value={dataset.id}
      onChange={e => { onSelectDataset(e.target.value); setPage(1); }}
      className={`text-xs font-medium rounded-lg border px-3 py-2 ${darkMode ? 'bg-slate-800 border-slate-700 text-slate-100' : 'bg-white border-slate-200 text-slate-800'}`}
     >
      {datasets.map(d => (
       <option key={d.id} value={d.id}>{d.name}</option>
      ))}
     </select>

     <button
      type="button"
      onClick={() => triggerFileDownload(rowsToCSV(activeRows), `${dataset.id}_${viewMode}.csv`)}
      className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] hover:bg-blue-700 text-white cursor-pointer"
     >
      <Download className="w-3.5 h-3.5" />
      Download {viewMode.toUpperCase()} CSV
     </button>
    </div>
   </div>

   {/* Metadata Banner */}
   <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3 text-xs">
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Source</div>
     <div className="font-semibold truncate mt-0.5">{dataset.sourceName}</div>
    </div>
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Created Date</div>
     <div className="font-semibold tabular-nums mt-0.5">{dataset.createdAt}</div>
    </div>
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Raw → Curated</div>
     <div className="font-semibold tabular-nums mt-0.5">{dataset.rawRows.length} → {dataset.curatedRows.length} rows</div>
    </div>
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Columns</div>
     <div className="font-semibold tabular-nums mt-0.5">{activeSchema.length} cols</div>
    </div>
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Quality Score</div>
     <div className="font-semibold text-emerald-500 tabular-nums mt-0.5">{dataset.curatedQuality.overallScore}% Gold</div>
    </div>
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Latest Execution</div>
     <div className="font-mono font-semibold text-[#0078D4] mt-0.5">#{dataset.latestExecutionId}</div>
    </div>
    <div className={`p-3 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-[10px] uppercase text-slate-400">Status</div>
     <div className="mt-0.5"><StatusBadge status={dataset.status} /></div>
    </div>
   </div>

   {/* Controls Bar */}
   <div className={`p-3.5 rounded-xl border flex flex-wrap items-center justify-between gap-3 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200'}`}>
    <div className="flex flex-wrap items-center gap-2">
     <div className="inline-flex rounded-lg p-0.5 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700">
      <button
       type="button"
       onClick={() => { setViewMode('curated'); setPage(1); }}
       className={`px-3 py-1.5 rounded-md text-xs font-semibold cursor-pointer ${
        viewMode === 'curated' ? 'bg-white dark:bg-slate-900 text-[#0078D4] shadow-sm' : 'text-slate-500'
       }`}
      >
       Curated Gold ({dataset.curatedRows.length})
      </button>
      <button
       type="button"
       onClick={() => { setViewMode('raw'); setPage(1); }}
       className={`px-3 py-1.5 rounded-md text-xs font-semibold cursor-pointer ${
        viewMode === 'raw' ? 'bg-white dark:bg-slate-900 text-amber-600 shadow-sm' : 'text-slate-500'
       }`}
      >
       Raw Bronze ({dataset.rawRows.length})
      </button>
      <button
       type="button"
       onClick={() => { setViewMode('quarantined'); setPage(1); }}
       className={`px-3 py-1.5 rounded-md text-xs font-semibold cursor-pointer ${
        viewMode === 'quarantined' ? 'bg-white dark:bg-slate-900 text-red-600 shadow-sm' : 'text-slate-500'
       }`}
      >
       Quarantined ({dataset.quarantinedRows.length})
      </button>
     </div>

     <div className="inline-flex rounded-lg p-0.5 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700">
      <button
       type="button"
       onClick={() => setActiveTab('grid')}
       className={`px-3 py-1.5 rounded-md text-xs font-medium cursor-pointer ${
        activeTab === 'grid' ? 'bg-white dark:bg-slate-900 text-slate-900 dark:text-white shadow-sm' : 'text-slate-500'
       }`}
      >
       Data Records Grid
      </button>
      <button
       type="button"
       onClick={() => setActiveTab('schema')}
       className={`px-3 py-1.5 rounded-md text-xs font-medium cursor-pointer ${
        activeTab === 'schema' ? 'bg-white dark:bg-slate-900 text-slate-900 dark:text-white shadow-sm' : 'text-slate-500'
       }`}
      >
       Schema & Column Statistics
      </button>
     </div>
    </div>

    <div className="flex flex-wrap items-center gap-2 text-xs">
     <div className="relative">
      <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-2.5" />
      <input
       type="text"
       placeholder="Search records..."
       value={searchQuery}
       onChange={e => { setSearchQuery(e.target.value); setPage(1); }}
       className={`pl-8 pr-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
      />
     </div>

     <select
      value={categoryFilter}
      onChange={e => { setCategoryFilter(e.target.value); setPage(1); }}
      className={`px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="ALL">All Categories</option>
      {categories.map(c => (
       <option key={c} value={c}>{c}</option>
      ))}
     </select>
    </div>
   </div>

   {/* Main Content Area: Grid vs Schema Stats */}
   {activeTab === 'grid' ? (
    <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="overflow-x-auto">
      <table className="w-full text-left text-xs border-collapse">
       <thead className={`uppercase text-[10px] tracking-wider border-b ${darkMode ? 'bg-slate-950/60 border-slate-800 text-slate-400' : 'bg-slate-50 border-slate-200 text-slate-500'}`}>
        <tr>
         {['order_id', 'order_date', 'product_name', 'category', 'quantity', 'unit_price', 'revenue', 'region', 'customer_segment', 'deal_size_tier'].map(col => (
          <th
           key={col}
           onClick={() => handleSort(col)}
           className="py-3 px-3.5 cursor-pointer hover:text-[#0078D4] select-none"
          >
           <span className="inline-flex items-center gap-1">
            {col}
            <ArrowUpDown className="w-3 h-3 opacity-60" />
           </span>
          </th>
         ))}
        </tr>
       </thead>
       <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
        {paginatedRows.map((row, idx) => (
         <tr key={idx} className="hover:bg-slate-50/70 dark:hover:bg-slate-800/40">
          <td className="py-2.5 px-3.5 font-mono font-semibold text-[#0078D4] dark:text-blue-400">{row.order_id}</td>
          <td className="py-2.5 px-3.5 font-mono">{String(row.order_date ?? 'NULL')}</td>
          <td className="py-2.5 px-3.5 font-medium">{row.product_name}</td>
          <td className="py-2.5 px-3.5">
           <span className="px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-800 font-mono text-[11px]">
            {String(row.category ?? 'NULL')}
           </span>
          </td>
          <td className="py-2.5 px-3.5 text-right font-mono">{String(row.quantity ?? 'NULL')}</td>
          <td className="py-2.5 px-3.5 text-right font-mono">
           {row.unit_price === null || row.unit_price === undefined ? 'NULL' : `$${Number(row.unit_price).toFixed(2)}`}
          </td>
          <td className="py-2.5 px-3.5 text-right font-mono font-semibold text-emerald-600 dark:text-emerald-400">
           {row.revenue !== undefined ? `$${Number(row.revenue).toLocaleString('en-US', { minimumFractionDigits: 2 })}` : '—'}
          </td>
          <td className="py-2.5 px-3.5">{String(row.region ?? 'NULL')}</td>
          <td className="py-2.5 px-3.5">{String(row.customer_segment ?? 'NULL')}</td>
          <td className="py-2.5 px-3.5 text-[11px] text-slate-500">
           {row._quarantineReason ? (
            <span className="text-red-500 font-semibold">{row._quarantineReason}</span>
           ) : (
            row.deal_size_tier || 'Raw Unprocessed'
           )}
          </td>
         </tr>
        ))}
       </tbody>
      </table>
     </div>

     {/* Pagination Footer */}
     <div className="px-4 py-3 border-t border-slate-100 dark:border-slate-800 flex flex-wrap items-center justify-between gap-2 text-xs">
      <div className="text-slate-500 tabular-nums">
       Showing {(currentPage - 1) * pageSize + 1} to {Math.min(currentPage * pageSize, filteredAndSortedRows.length)} of {filteredAndSortedRows.length} records
      </div>
      <div className="flex items-center gap-2">
       <select
        value={pageSize}
        onChange={e => { setPageSize(Number(e.target.value)); setPage(1); }}
        className={`px-2 py-1 rounded border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        <option value={12}>12 / page</option>
        <option value={25}>25 / page</option>
        <option value={50}>50 / page</option>
       </select>
       <button
        type="button"
        disabled={currentPage <= 1}
        onClick={() => setPage(p => Math.max(1, p - 1))}
        className="px-2.5 py-1 rounded border border-slate-200 dark:border-slate-700 disabled:opacity-40 cursor-pointer"
       >
        Prev
       </button>
       <span className="font-mono tabular-nums">Page {currentPage} / {totalPages}</span>
       <button
        type="button"
        disabled={currentPage >= totalPages}
        onClick={() => setPage(p => Math.min(totalPages, p + 1))}
        className="px-2.5 py-1 rounded border border-slate-200 dark:border-slate-700 disabled:opacity-40 cursor-pointer"
       >
        Next
       </button>
      </div>
     </div>
    </div>
   ) : (
    /* Schema & Column Statistics Tab */
    <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="overflow-x-auto">
      <table className="w-full text-left text-xs">
       <thead className={`uppercase text-[10px] ${darkMode ? 'bg-slate-950/60 text-slate-400' : 'bg-slate-50 text-slate-500'}`}>
        <tr>
         <th className="py-3 px-4">Column Name</th>
         <th className="py-3 px-4">Data Type</th>
         <th className="py-3 px-4 text-right">Null Count</th>
         <th className="py-3 px-4 text-right">Cardinality (Unique)</th>
         <th className="py-3 px-4 text-right">Min</th>
         <th className="py-3 px-4 text-right">Max</th>
         <th className="py-3 px-4 text-right">Mean</th>
         <th className="py-3 px-4">Sample Values</th>
        </tr>
       </thead>
       <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
        {activeSchema.map(col => (
         <tr key={col.name}>
          <td className="py-2.5 px-4 font-mono font-semibold">{col.name}</td>
          <td className="py-2.5 px-4 font-mono text-[#0078D4] dark:text-blue-400">{col.inferredType}</td>
          <td className="py-2.5 px-4 text-right">{col.nullCount}</td>
          <td className="py-2.5 px-4 text-right">{col.uniqueCount}</td>
          <td className="py-2.5 px-4 text-right font-mono">{col.min !== undefined ? String(col.min) : '—'}</td>
          <td className="py-2.5 px-4 text-right font-mono">{col.max !== undefined ? String(col.max) : '—'}</td>
          <td className="py-2.5 px-4 text-right font-mono">{col.mean !== undefined ? col.mean.toLocaleString() : '—'}</td>
          <td className="py-2.5 px-4 text-slate-500 truncate max-w-xs">{col.sampleValues.join(', ')}</td>
         </tr>
        ))}
       </tbody>
      </table>
     </div>
    </div>
   )}
  </div>
 );
};

// ============================================================================
// 13. ANALYTICS WORKSPACE, REPORT BUILDER & DATA CATALOG VIEWS
// ============================================================================

const AnalyticsDashboardView: React.FC<{
 datasets: DatasetEntity[];
 activeDatasetId: string;
 onSelectDataset: (id: string) => void;
 onSaveReportFromAnalytics: (report: SavedAnalyticsReport) => void;
 onNavigate: (page: NavPageId) => void;
 darkMode: boolean;
}> = ({ datasets, activeDatasetId, onSelectDataset, onSaveReportFromAnalytics, onNavigate, darkMode }) => {
 const dataset = useMemo(
  () => datasets.find(d => d.id === activeDatasetId) || datasets[0],
  [datasets, activeDatasetId]
 );

 const [dateRange, setDateRange] = useState<string>('ALL');
 const [categoryFilter, setCategoryFilter] = useState<string>('ALL');
 const [regionFilter, setRegionFilter] = useState<string>('ALL');
 const [groupByField, setGroupByField] = useState<string>('category');
 const [aggMetric, setAggMetric] = useState<'sum' | 'mean' | 'count' | 'min' | 'max'>('sum');
 const [chartType, setChartType] = useState<'bar' | 'line' | 'area' | 'donut' | 'stacked'>('bar');

 const curatedRows = dataset.curatedRows;

 const categories = useMemo(
  () => Array.from(new Set(curatedRows.map(r => String(r.category || 'Electronics')))),
  [curatedRows]
 );
 const regions = useMemo(
  () => Array.from(new Set(curatedRows.map(r => String(r.region || 'North America')))),
  [curatedRows]
 );

 const filteredRows = useMemo(() => {
  return curatedRows.filter(r => {
   if (dateRange !== 'ALL' && !String(r.order_date || '').startsWith(dateRange)) return false;
   if (categoryFilter !== 'ALL' && String(r.category) !== categoryFilter) return false;
   if (regionFilter !== 'ALL' && String(r.region) !== regionFilter) return false;
   return true;
  });
 }, [curatedRows, dateRange, categoryFilter, regionFilter]);

 const aggregatedData = useMemo(() => {
  const groups = new Map<string, { rows: DataRow[]; revs: number[]; qtys: number[] }>();
  filteredRows.forEach(r => {
   const k = String(r[groupByField] ?? 'Other');
   const rev = Number(r.revenue ?? Number(r.quantity || 0) * Number(r.unit_price || 0));
   const qty = Number(r.quantity || 0);
   if (!groups.has(k)) groups.set(k, { rows: [], revs: [], qtys: [] });
   const g = groups.get(k)!;
   g.rows.push(r);
   g.revs.push(rev);
   g.qtys.push(qty);
  });

  return Array.from(groups.entries()).map(([groupKey, g]) => {
   const sumRev = Number(g.revs.reduce((a, b) => a + b, 0).toFixed(2));
   const count = g.revs.length;
   const meanRev = count > 0 ? Number((sumRev / count).toFixed(2)) : 0;
   const minRev = count > 0 ? Number(Math.min(...g.revs).toFixed(2)) : 0;
   const maxRev = count > 0 ? Number(Math.max(...g.revs).toFixed(2)) : 0;
   const totalUnits = g.qtys.reduce((a, b) => a + b, 0);

   // Segment breakdown for stacked bar
   const enterpriseRev = Number(
    g.rows
     .filter(x => x.customer_segment === 'Enterprise')
     .reduce((a, x) => a + Number(x.revenue || 0), 0)
     .toFixed(2)
   );
   const midMarketRev = Number(
    g.rows
     .filter(x => x.customer_segment === 'Mid-Market')
     .reduce((a, x) => a + Number(x.revenue || 0), 0)
     .toFixed(2)
   );
   const smbRev = Number(
    g.rows
     .filter(x => x.customer_segment === 'SMB')
     .reduce((a, x) => a + Number(x.revenue || 0), 0)
     .toFixed(2)
   );

   let metricValue = sumRev;
   if (aggMetric === 'mean') metricValue = meanRev;
   if (aggMetric === 'count') metricValue = count;
   if (aggMetric === 'min') metricValue = minRev;
   if (aggMetric === 'max') metricValue = maxRev;

   return {
    groupKey,
    metricValue,
    sumRev,
    meanRev,
    count,
    minRev,
    maxRev,
    totalUnits,
    Enterprise: enterpriseRev,
    'Mid-Market': midMarketRev,
    SMB: smbRev
   };
  }).sort((a, b) => b.metricValue - a.metricValue);
 }, [filteredRows, groupByField, aggMetric]);

 const totalRevenue = useMemo(
  () => filteredRows.reduce((a, r) => a + Number(r.revenue || 0), 0),
  [filteredRows]
 );
 const avgOrderVal = filteredRows.length > 0 ? totalRevenue / filteredRows.length : 0;
 const totalUnitsSold = useMemo(
  () => filteredRows.reduce((a, r) => a + Number(r.quantity || 0), 0),
  [filteredRows]
 );

 const handleDownloadChartCSV = () => {
  const headers = ['Group', 'MetricValue', 'TotalRevenue', 'AvgRevenue', 'OrderCount', 'MinRevenue', 'MaxRevenue', 'TotalUnits'];
  const lines = [headers.join(',')];
  aggregatedData.forEach(d => {
   lines.push([`"${d.groupKey}"`, d.metricValue, d.sumRev, d.meanRev, d.count, d.minRev, d.maxRev, d.totalUnits].join(','));
  });
  triggerFileDownload(lines.join('\n'), `analytics_${dataset.id}_by_${groupByField}.csv`);
 };

 const handleQuickSaveReport = () => {
  const newRep: SavedAnalyticsReport = {
   id: `REP-${Date.now().toString().slice(-4)}`,
   title: `${dataset.name} — ${aggMetric.toUpperCase()} by ${groupByField}`,
   datasetId: dataset.id,
   datasetName: dataset.name,
   createdAt: '2026-10-21 13:05 IST',
   notes: `Generated from interactive Analytics Workspace with filters: Period=${dateRange}, Category=${categoryFilter}, Region=${regionFilter}.`,
   filters: {
    dateRange,
    category: categoryFilter,
    region: regionFilter,
    segment: 'ALL',
    groupBy: groupByField,
    metric: aggMetric
   },
   summaryMetrics: {
    totalRevenue: Number(totalRevenue.toFixed(2)),
    totalOrders: filteredRows.length,
    avgOrderValue: Number(avgOrderVal.toFixed(2)),
    topCategory: aggregatedData[0]?.groupKey || 'N/A',
    qualityScore: dataset.curatedQuality.overallScore
   }
  };
  onSaveReportFromAnalytics(newRep);
  onNavigate('reports');
 };

 const COLORS = ['#0078D4', '#4F46E5', '#10B981', '#F59E0B', '#EC4899', '#06B6D4'];

 return (
  <div className="space-y-6">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Interactive Analytics Workspace</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Slice, group, and aggregate curated datasets dynamically across multiple chart types and export underlying tabular results.
     </p>
    </div>

    <div className="flex items-center gap-2">
     <button
      type="button"
      onClick={handleDownloadChartCSV}
      className={`inline-flex items-center gap-1.5 px-3 py-2 rounded-lg border text-xs font-medium cursor-pointer ${
       darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
      }`}
     >
      <Download className="w-3.5 h-3.5 text-[#0078D4]" />
      Download Chart Data (CSV)
     </button>
     <button
      type="button"
      onClick={handleQuickSaveReport}
      className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] hover:bg-blue-700 text-white cursor-pointer"
     >
      <Save className="w-3.5 h-3.5" />
      Save View as Report
     </button>
    </div>
   </div>

   {/* Interactive Filter & Grouping Controls */}
   <div className={`p-4 rounded-xl border grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 text-xs ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div>
     <label className="block text-[10px] uppercase text-slate-400 font-semibold mb-1">Dataset</label>
     <select
      value={dataset.id}
      onChange={e => onSelectDataset(e.target.value)}
      className={`w-full px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      {datasets.map(d => (
       <option key={d.id} value={d.id}>{d.name}</option>
      ))}
     </select>
    </div>

    <div>
     <label className="block text-[10px] uppercase text-slate-400 font-semibold mb-1">Date Range</label>
     <select
      value={dateRange}
      onChange={e => setDateRange(e.target.value)}
      className={`w-full px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="ALL">All Dates</option>
      <option value="2026-08">August 2026</option>
      <option value="2026-09">September 2026</option>
      <option value="2026-10">October 2026</option>
     </select>
    </div>

    <div>
     <label className="block text-[10px] uppercase text-slate-400 font-semibold mb-1">Category Filter</label>
     <select
      value={categoryFilter}
      onChange={e => setCategoryFilter(e.target.value)}
      className={`w-full px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="ALL">All Categories</option>
      {categories.map(c => (
       <option key={c} value={c}>{c}</option>
      ))}
     </select>
    </div>

    <div>
     <label className="block text-[10px] uppercase text-slate-400 font-semibold mb-1">Region Filter</label>
     <select
      value={regionFilter}
      onChange={e => setRegionFilter(e.target.value)}
      className={`w-full px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="ALL">All Regions</option>
      {regions.map(r => (
       <option key={r} value={r}>{r}</option>
      ))}
     </select>
    </div>

    <div>
     <label className="block text-[10px] uppercase text-slate-400 font-semibold mb-1">Group By Dimension</label>
     <select
      value={groupByField}
      onChange={e => setGroupByField(e.target.value)}
      className={`w-full px-2.5 py-1.5 rounded-lg border font-medium text-[#0078D4] ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="category">Product Category</option>
      <option value="region">Geographic Region</option>
      <option value="customer_segment">Customer Segment</option>
      <option value="order_month">Order Month</option>
      <option value="deal_size_tier">Deal Size Tier</option>
      <option value="payment_method">Payment Method</option>
     </select>
    </div>

    <div>
     <label className="block text-[10px] uppercase text-slate-400 font-semibold mb-1">Aggregation Metric</label>
     <select
      value={aggMetric}
      onChange={e => setAggMetric(e.target.value as any)}
      className={`w-full px-2.5 py-1.5 rounded-lg border font-medium ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value="sum">SUM (Total Revenue)</option>
      <option value="mean">MEAN (Average Order Value)</option>
      <option value="count">COUNT (Order Volume)</option>
      <option value="min">MIN (Smallest Order)</option>
      <option value="max">MAX (Largest Order)</option>
     </select>
    </div>
   </div>

   {/* Filtered Summary Metric Cards */}
   <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Filtered Revenue</div>
     <div className="text-2xl font-bold text-[#0078D4] tabular-nums mt-1">
      ${totalRevenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
     </div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Filtered Order Count</div>
     <div className="text-2xl font-bold tabular-nums mt-1">{filteredRows.length} orders</div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Average Order Value</div>
     <div className="text-2xl font-bold text-emerald-500 tabular-nums mt-1">
      ${avgOrderVal.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
     </div>
    </div>
    <div className={`p-4 rounded-xl border ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
     <div className="text-xs text-slate-400">Total Units Sold</div>
     <div className="text-2xl font-bold text-indigo-500 tabular-nums mt-1">{totalUnitsSold.toLocaleString()} units</div>
    </div>
   </div>

   {/* Primary Interactive Visualization + Chart Type Toggle */}
   <div className={`rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
     <div>
      <h3 className="font-semibold text-base">
       {aggMetric.toUpperCase()} by {groupByField}
      </h3>
      <p className="text-xs text-slate-500">
       Toggle chart types to inspect distribution, trend, or customer segment composition
      </p>
     </div>
     <div className="inline-flex rounded-lg p-0.5 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-xs">
      {(['bar', 'stacked', 'line', 'area', 'donut'] as const).map(t => (
       <button
        key={t}
        type="button"
        onClick={() => setChartType(t)}
        className={`px-3 py-1 rounded-md font-medium capitalize cursor-pointer ${
         chartType === t ? 'bg-white dark:bg-slate-900 text-[#0078D4] shadow-sm' : 'text-slate-500'
        }`}
       >
        {t}
       </button>
      ))}
     </div>
    </div>

    <div className="h-72">
     <ResponsiveContainer width="100%" height="100%">
      {chartType === 'bar' ? (
       <BarChart data={aggregatedData}>
        <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#f1f5f9'} />
        <XAxis dataKey="groupKey" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <RechartsTooltip />
        <Bar dataKey="metricValue" name={aggMetric.toUpperCase()} fill="#0078D4" radius={[6, 6, 0, 0]} />
       </BarChart>
      ) : chartType === 'stacked' ? (
       <BarChart data={aggregatedData}>
        <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#f1f5f9'} />
        <XAxis dataKey="groupKey" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <RechartsTooltip />
        <Legend />
        <Bar dataKey="Enterprise" stackId="a" fill="#0078D4" />
        <Bar dataKey="Mid-Market" stackId="a" fill="#4F46E5" />
        <Bar dataKey="SMB" stackId="a" fill="#10B981" radius={[4, 4, 0, 0]} />
       </BarChart>
      ) : chartType === 'line' ? (
       <LineChart data={aggregatedData}>
        <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#f1f5f9'} />
        <XAxis dataKey="groupKey" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <RechartsTooltip />
        <Line type="monotone" dataKey="metricValue" stroke="#0078D4" strokeWidth={3} dot={{ r: 5 }} />
       </LineChart>
      ) : chartType === 'area' ? (
       <AreaChart data={aggregatedData}>
        <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#1e293b' : '#f1f5f9'} />
        <XAxis dataKey="groupKey" tick={{ fontSize: 11 }} />
        <YAxis tick={{ fontSize: 11 }} />
        <RechartsTooltip />
        <Area type="monotone" dataKey="metricValue" stroke="#4F46E5" fill="#4F46E5" fillOpacity={0.2} strokeWidth={2.5} />
       </AreaChart>
      ) : (
       <PieChart>
        <Pie
         data={aggregatedData}
         dataKey="metricValue"
         nameKey="groupKey"
         cx="50%"
         cy="50%"
         innerRadius={60}
         outerRadius={95}
         paddingAngle={3}
        >
         {aggregatedData.map((_, idx) => (
          <Cell key={idx} fill={COLORS[idx % COLORS.length]} />
         ))}
        </Pie>
        <RechartsTooltip />
        <Legend />
       </PieChart>
      )}
     </ResponsiveContainer>
    </div>
   </div>

   {/* Aggregated Summary Table */}
   <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div className="px-5 py-3.5 border-b border-slate-100 dark:border-slate-800 font-semibold text-sm">
     Aggregated Rollup Table (Grouped by {groupByField})
    </div>
    <div className="overflow-x-auto">
     <table className="w-full text-left text-xs">
      <thead className={`uppercase text-[10px] ${darkMode ? 'bg-slate-950/60 text-slate-400' : 'bg-slate-50 text-slate-500'}`}>
       <tr>
        <th className="py-3 px-4">{groupByField}</th>
        <th className="py-3 px-4 text-right">Order Count</th>
        <th className="py-3 px-4 text-right">Total Units</th>
        <th className="py-3 px-4 text-right">SUM (Revenue)</th>
        <th className="py-3 px-4 text-right">MEAN (AOV)</th>
        <th className="py-3 px-4 text-right">MIN Order</th>
        <th className="py-3 px-4 text-right">MAX Order</th>
       </tr>
      </thead>
      <tbody className="divide-y divide-slate-100 dark:divide-slate-800 tabular-nums">
       {aggregatedData.map(row => (
        <tr key={row.groupKey}>
         <td className="py-2.5 px-4 font-semibold">{row.groupKey}</td>
         <td className="py-2.5 px-4 text-right">{row.count}</td>
         <td className="py-2.5 px-4 text-right">{row.totalUnits}</td>
         <td className="py-2.5 px-4 text-right font-mono font-semibold text-[#0078D4] dark:text-blue-400">
          ${row.sumRev.toLocaleString('en-US', { minimumFractionDigits: 2 })}
         </td>
         <td className="py-2.5 px-4 text-right font-mono">${row.meanRev.toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
         <td className="py-2.5 px-4 text-right font-mono">${row.minRev.toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
         <td className="py-2.5 px-4 text-right font-mono">${row.maxRev.toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
        </tr>
       ))}
      </tbody>
     </table>
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 14. REPORTS BUILDER VIEW
// ============================================================================

const ReportsView: React.FC<{
 reports: SavedAnalyticsReport[];
 datasets: DatasetEntity[];
 onCreateReport: (report: SavedAnalyticsReport) => void;
 darkMode: boolean;
}> = ({ reports, datasets, onCreateReport, darkMode }) => {
 const [selectedReportId, setSelectedReportId] = useState<string>(reports[0]?.id || '');
 const [title, setTitle] = useState('Q3–Q4 Executive Revenue & Data Quality Audit');
 const [datasetId, setDatasetId] = useState(datasets[0]?.id || '');
 const [groupBy, setGroupBy] = useState('category');
 const [metric, setMetric] = useState('sum');
 const [notes, setNotes] = useState('Summarizes curated ADLS Gen2 Gold transaction metrics after deduplication and schema enforcement.');

 const activeReport = useMemo(
  () => reports.find(r => r.id === selectedReportId) || reports[0],
  [reports, selectedReportId]
 );

 const handleGenerateReport = (e: React.FormEvent) => {
  e.preventDefault();
  const ds = datasets.find(d => d.id === datasetId) || datasets[0];
  const totalRevenue = ds.curatedRows.reduce((a, r) => a + Number(r.revenue || 0), 0);
  const avgOrderValue = ds.curatedRows.length > 0 ? totalRevenue / ds.curatedRows.length : 0;

  const newRep: SavedAnalyticsReport = {
   id: `REP-${Date.now().toString().slice(-4)}`,
   title: title.trim() || 'Untitled Analytics Report',
   datasetId: ds.id,
   datasetName: ds.name,
   createdAt: '2026-10-21 13:06 IST',
   notes,
   filters: {
    dateRange: 'ALL',
    category: 'ALL',
    region: 'ALL',
    segment: 'ALL',
    groupBy,
    metric
   },
   summaryMetrics: {
    totalRevenue: Number(totalRevenue.toFixed(2)),
    totalOrders: ds.curatedRows.length,
    avgOrderValue: Number(avgOrderValue.toFixed(2)),
    topCategory: 'Software Licenses',
    qualityScore: ds.curatedQuality.overallScore
   }
  };
  onCreateReport(newRep);
  setSelectedReportId(newRep.id);
 };

 const handleDownloadReportMarkdown = (rep: SavedAnalyticsReport) => {
  const md = `# ${rep.title}\n\n- **Report ID:** ${rep.id}\n- **Dataset:** ${rep.datasetName}\n- **Generated Timestamp:** ${rep.createdAt}\n- **Data Quality Score:** ${rep.summaryMetrics.qualityScore}%\n\n## Executive Summary\n${rep.notes}\n\n## Key Business Metrics\n- **Total Curated Revenue:** $${rep.summaryMetrics.totalRevenue.toLocaleString()}\n- **Validated Order Volume:** ${rep.summaryMetrics.totalOrders}\n- **Average Order Value (AOV):** $${rep.summaryMetrics.avgOrderValue.toLocaleString()}\n- **Primary Grouping Dimension:** ${rep.filters.groupBy} (${rep.filters.metric.toUpperCase()})\n`;
  triggerFileDownload(md, `${rep.id}_analytics_report.md`, 'text/markdown;charset=utf-8;');
 };

 return (
  <div className="space-y-6">
   <div>
    <h1 className="text-xl font-bold tracking-tight">Analytics Report Builder & Archive</h1>
    <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
     Create, save, reopen, and export executive analytics snapshots backed by curated Gold datasets.
    </p>
   </div>

   <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
    {/* Report Builder Form */}
    <form
     onSubmit={handleGenerateReport}
     className={`lg:col-span-5 rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}
    >
     <h3 className="font-semibold text-sm pb-2 border-b border-slate-100 dark:border-slate-800">
      Create New Analytics Report
     </h3>

     <div className="space-y-3 text-xs">
      <div>
       <label className="block font-medium mb-1">Report Title</label>
       <input
        type="text"
        value={title}
        onChange={e => setTitle(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>

      <div>
       <label className="block font-medium mb-1">Target Dataset</label>
       <select
        value={datasetId}
        onChange={e => setDatasetId(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       >
        {datasets.map(d => (
         <option key={d.id} value={d.id}>{d.name}</option>
        ))}
       </select>
      </div>

      <div className="grid grid-cols-2 gap-3">
       <div>
        <label className="block font-medium mb-1">Group By Field</label>
        <select
         value={groupBy}
         onChange={e => setGroupBy(e.target.value)}
         className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        >
         <option value="category">category</option>
         <option value="region">region</option>
         <option value="customer_segment">customer_segment</option>
         <option value="order_month">order_month</option>
        </select>
       </div>
       <div>
        <label className="block font-medium mb-1">Metric</label>
        <select
         value={metric}
         onChange={e => setMetric(e.target.value)}
         className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        >
         <option value="sum">SUM(revenue)</option>
         <option value="mean">MEAN(revenue)</option>
         <option value="count">COUNT(orders)</option>
        </select>
       </div>
      </div>

      <div>
       <label className="block font-medium mb-1">Analyst Commentary & Notes</label>
       <textarea
        rows={3}
        value={notes}
        onChange={e => setNotes(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>

      <button
       type="submit"
       className="w-full py-2.5 px-4 rounded-xl bg-[#0078D4] hover:bg-blue-700 text-white font-semibold text-xs cursor-pointer"
      >
       Generate & Save Report
      </button>
     </div>
    </form>

    {/* Saved Reports List & Preview */}
    <div className="lg:col-span-7 space-y-4">
     <div className={`rounded-xl border p-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
      <h3 className="font-semibold text-xs uppercase tracking-wider text-slate-400 mb-3">
       Saved Reports ({reports.length})
      </h3>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
       {reports.map(r => (
        <button
         key={r.id}
         type="button"
         onClick={() => setSelectedReportId(r.id)}
         className={`text-left p-3 rounded-xl border transition-all cursor-pointer ${
          r.id === activeReport?.id
           ? 'border-[#0078D4] bg-blue-50/50 dark:bg-blue-950/30 ring-2 ring-[#0078D4]/20'
           : darkMode
           ? 'border-slate-800 hover:border-slate-700'
           : 'border-slate-200 hover:border-slate-300'
         }`}
        >
         <div className="flex items-center justify-between text-[11px] font-mono text-[#0078D4]">
          <span>{r.id}</span>
          <span>{r.createdAt}</span>
         </div>
         <div className="font-semibold text-xs mt-1 truncate">{r.title}</div>
         <div className="text-[11px] text-slate-500 mt-0.5">{r.datasetName}</div>
        </button>
       ))}
      </div>
     </div>

     {activeReport && (
      <div className={`rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
       <div className="flex flex-wrap items-center justify-between gap-2 pb-3 border-b border-slate-100 dark:border-slate-800">
        <div>
         <span className="text-xs font-mono text-[#0078D4]">{activeReport.id} • {activeReport.createdAt}</span>
         <h3 className="font-bold text-base mt-0.5">{activeReport.title}</h3>
        </div>
        <div className="flex items-center gap-2">
         <button
          type="button"
          onClick={() => handleDownloadReportMarkdown(activeReport)}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-[#0078D4] text-white text-xs font-semibold hover:bg-blue-700 cursor-pointer"
         >
          <Download className="w-3.5 h-3.5" />
          Export Report (.md)
         </button>
        </div>
       </div>

       <p className="text-xs text-slate-600 dark:text-slate-300 leading-relaxed">
        {activeReport.notes}
       </p>

       <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
        <div className={`p-3 rounded-lg border ${darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
         <div className="text-slate-400">Total Revenue</div>
         <div className="font-bold text-sm mt-0.5 tabular-nums">${activeReport.summaryMetrics.totalRevenue.toLocaleString()}</div>
        </div>
        <div className={`p-3 rounded-lg border ${darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
         <div className="text-slate-400">Total Orders</div>
         <div className="font-bold text-sm mt-0.5 tabular-nums">{activeReport.summaryMetrics.totalOrders}</div>
        </div>
        <div className={`p-3 rounded-lg border ${darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
         <div className="text-slate-400">Average Order Value</div>
         <div className="font-bold text-sm mt-0.5 tabular-nums">${activeReport.summaryMetrics.avgOrderValue.toLocaleString()}</div>
        </div>
        <div className={`p-3 rounded-lg border ${darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
         <div className="text-slate-400">Dataset Quality</div>
         <div className="font-bold text-sm text-emerald-500 mt-0.5 tabular-nums">{activeReport.summaryMetrics.qualityScore}%</div>
        </div>
       </div>
      </div>
     )}
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 15. DATA CATALOG VIEW
// ============================================================================

const DataCatalogView: React.FC<{
 datasets: DatasetEntity[];
 onNavigate: (page: NavPageId) => void;
 darkMode: boolean;
}> = ({ datasets, onNavigate, darkMode }) => {
 const catalogAssets = useMemo(() => {
  return datasets.flatMap(d => [
   {
    assetId: `bronze.${d.id}_raw`,
    name: `${d.name} (Raw Landing)`,
    layer: 'Bronze Layer (ADLS Gen2)',
    rows: d.rawRows.length,
    quality: d.rawQuality.overallScore,
    owner: 'Data Engineering Ingestion Service',
    piiClassification: 'Synthetic / Non-PII',
    sla: 'Raw Append-Only'
   },
   {
    assetId: `gold.${d.id}_curated`,
    name: `${d.name} (Curated Delta Table)`,
    layer: 'Gold Layer (Delta Lake / Synapse)',
    rows: d.curatedRows.length,
    quality: d.curatedQuality.overallScore,
    owner: 'Analytics Engineering Team',
    piiClassification: 'Synthetic / Non-PII Verified',
    sla: '99.5% Quality Gate Enforced'
   }
  ]);
 }, [datasets]);

 return (
  <div className="space-y-6">
   <div className="flex items-center justify-between">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Enterprise Data Catalog & Lineage Registry</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Microsoft Purview-inspired metadata registry tracking Bronze and Gold medallion assets, owners, and governance tags.
     </p>
    </div>
   </div>

   <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
    {catalogAssets.map(asset => (
     <div
      key={asset.assetId}
      className={`rounded-xl border p-5 space-y-3 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}
     >
      <div className="flex items-center justify-between">
       <span className="font-mono text-xs font-semibold text-[#0078D4] dark:text-blue-400">{asset.assetId}</span>
       <StatusBadge status={asset.layer.includes('Gold') ? 'Gold Curated' : 'Bronze Raw'} />
      </div>
      <h3 className="font-bold text-sm">{asset.name}</h3>
      <div className="grid grid-cols-2 gap-2 text-xs text-slate-500 dark:text-slate-400">
       <div>Storage Layer: <strong className="text-slate-700 dark:text-slate-200">{asset.layer}</strong></div>
       <div>Record Count: <strong className="text-slate-700 dark:text-slate-200 tabular-nums">{asset.rows} rows</strong></div>
       <div>Quality Score: <strong className="text-emerald-500 tabular-nums">{asset.quality}%</strong></div>
       <div>Privacy Tag: <strong className="text-slate-700 dark:text-slate-200">{asset.piiClassification}</strong></div>
      </div>
      <div className="pt-2 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between text-xs">
       <span className="text-slate-400">Steward: {asset.owner}</span>
       <button
        type="button"
        onClick={() => onNavigate('explorer')}
        className="font-semibold text-[#0078D4] hover:underline cursor-pointer"
       >
        Open in Explorer →
       </button>
      </div>
     </div>
    ))}
   </div>
  </div>
 );
};

// ============================================================================
// 16. MICROSOFT AZURE ARCHITECTURE & CLOUD OPERATIONS VIEWS
// ============================================================================

interface AzureComponentSpec {
 id: string;
 title: string;
 azureProduct: string;
 layer: string;
 responsibility: string;
 inputSpec: string;
 outputSpec: string;
 integrationStatus: 'Simulated (Demo Mode)' | 'Configured (Server-Side Managed Identity)';
 configSnippet: string;
}

const AZURE_ARCHITECTURE_COMPONENTS: AzureComponentSpec[] = [
 {
  id: 'az-sources',
  title: '1. Data Sources',
  azureProduct: 'CSV / JSON / Operational Read Replicas',
  layer: 'Source Systems',
  responsibility: 'Supplies transactional e-commerce order files, JSON partner feeds, and operational database snapshots.',
  inputSpec: 'Raw user file uploads (.csv, .json) or JDBC/TDS read-only queries',
  outputSpec: 'Unvalidated byte streams & structured record batches',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `source_type: "DelimitedText"\ncompression: "None"\nencodingName: "UTF-8"\nfirstRowAsHeader: true`
 },
 {
  id: 'az-adf',
  title: '2. Ingestion Orchestration',
  azureProduct: 'Azure Data Factory (ADF V2)',
  layer: 'Ingestion & Orchestration',
  responsibility: 'Executes Copy Activities and triggers downstream Databricks transformation notebooks on schedule or event arrival.',
  inputSpec: 'Source dataset streams + column mapping definitions',
  outputSpec: 'Raw Parquet / CSV files written to ADLS Gen2 Bronze container',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `resource "azurerm_data_factory_pipeline" "ingest_orders" {\n  name = "pl_ingest_ecommerce_bronze"\n  activities_json = "[CopyActivity -> DatabricksNotebook]"\n}`
 },
 {
  id: 'az-adls-raw',
  title: '3. Raw Storage (Bronze)',
  azureProduct: 'Azure Data Lake Storage Gen2 — Raw Layer',
  layer: 'Medallion Bronze Storage',
  responsibility: 'Preserves immutable raw ingested files with hierarchical namespace enabled so raw data is never silently overwritten.',
  inputSpec: 'ADF Copy Activity sink streams',
  outputSpec: 'abfss://raw@azurepulsedatalake.dfs.core.windows.net/bronze/orders/',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `storage_account: "azurepulsedatalake"\nis_hns_enabled: true\ncontainer: "bronze-raw-landing"`
 },
 {
  id: 'az-databricks',
  title: '4. Validation & Transformation',
  azureProduct: 'Azure Databricks (PySpark / Pandas Engine)',
  layer: 'Compute & Quality Gate',
  responsibility: 'Performs schema validation, exact & business-key deduplication, category normalization, null imputation, and feature engineering.',
  inputSpec: 'ADLS Gen2 Bronze dataset records',
  outputSpec: 'Validated Gold records + Quarantined error rows + Quality metrics',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `cluster_runtime: "14.3.x-scala2.12"\nspark_conf: {\n  "spark.databricks.delta.schema.autoMerge.enabled": "true"\n}`
 },
 {
  id: 'az-adls-curated',
  title: '5. Curated Storage (Gold)',
  azureProduct: 'Azure Data Lake Storage Gen2 — Curated Layer',
  layer: 'Medallion Gold Delta Lake',
  responsibility: 'Stores acid-compliant, deduplicated, 100% schema-conformant Gold datasets ready for enterprise analytics.',
  inputSpec: 'Transformed DataFrame output from Databricks / Pandas engine',
  outputSpec: 'abfss://curated@azurepulsedatalake.dfs.core.windows.net/gold/fact_orders/',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `CREATE TABLE gold.fact_ecommerce_orders\nUSING DELTA\nLOCATION 'abfss://curated@azurepulsedatalake.dfs.core.windows.net/gold/fact_orders'`
 },
 {
  id: 'az-synapse',
  title: '6. Serving & SQL Analytics',
  azureProduct: 'Azure Synapse Analytics (Serverless & Dedicated SQL)',
  layer: 'Analytical Serving Layer',
  responsibility: 'Exposes low-latency T-SQL views over Gold Delta Lake tables for dimensional slicing, regional rollups, and reporting.',
  inputSpec: 'Delta Lake Gold tables on ADLS Gen2',
  outputSpec: 'Aggregated KPI views & dimensional cubes',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `SELECT category, region, SUM(revenue) AS total_rev\nFROM OPENROWSET(BULK 'https://azurepulsedatalake.dfs.core.windows.net/gold/fact_orders/', FORMAT = 'DELTA') AS r\nGROUP BY category, region;`
 },
 {
  id: 'az-monitor',
  title: '7. Visualization & Monitoring',
  azureProduct: 'Azure Monitor, Log Analytics & Reporting Layer',
  layer: 'Observability & BI',
  responsibility: 'Captures pipeline execution telemetry, data quality threshold alerts, stage latency metrics, and interactive executive dashboards.',
  inputSpec: 'ADF diagnostic logs, Databricks cluster metrics, Quality Engine alerts',
  outputSpec: 'Real-time alerts, SLA dashboards, and downloadable analytics reports',
  integrationStatus: 'Simulated (Demo Mode)',
  configSnippet: `workspace: "law-azurepulse-prod"\nretention_in_days: 90\nalert_rule: "QualityScore < 95.0"`
 }
];

const AzureArchitectureView: React.FC<{
 integrationMode: 'demo' | 'configurable';
 onToggleIntegrationMode: (mode: 'demo' | 'configurable') => void;
 onNavigate: (page: NavPageId) => void;
 darkMode: boolean;
}> = ({ integrationMode, onToggleIntegrationMode, onNavigate, darkMode }) => {
 const [selectedCompId, setSelectedCompId] = useState<string>('az-databricks');
 const selectedComp = useMemo(
  () => AZURE_ARCHITECTURE_COMPONENTS.find(c => c.id === selectedCompId) || AZURE_ARCHITECTURE_COMPONENTS[0],
  [selectedCompId]
 );

 return (
  <div className="space-y-6">
   {/* Header & Honest Demo Mode Banner */}
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Microsoft Azure Cloud Data Platform Architecture</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      End-to-end Medallion Lakehouse reference architecture (Bronze → Silver → Gold) with interactive component inspection.
     </p>
    </div>

    <div className="inline-flex rounded-xl p-1 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-xs">
     <button
      type="button"
      onClick={() => onToggleIntegrationMode('demo')}
      className={`px-3 py-1.5 rounded-lg font-semibold cursor-pointer ${
       integrationMode === 'demo' ? 'bg-white dark:bg-slate-900 text-amber-600 shadow-sm' : 'text-slate-500'
      }`}
     >
      Demo Mode (Simulated)
     </button>
     <button
      type="button"
      onClick={() => onToggleIntegrationMode('configurable')}
      className={`px-3 py-1.5 rounded-lg font-semibold cursor-pointer ${
       integrationMode === 'configurable' ? 'bg-white dark:bg-slate-900 text-[#0078D4] shadow-sm' : 'text-slate-500'
      }`}
     >
      Configurable Integration Mode
     </button>
    </div>
   </div>

   {/* Mandatory Honest Simulation Status Banner */}
   <div className="p-4 rounded-xl border border-amber-300/80 bg-amber-50/80 dark:bg-amber-950/30 dark:border-amber-800/70 flex flex-wrap items-center justify-between gap-3 text-xs">
    <div className="flex items-center gap-2.5">
     <Cloud className="w-5 h-5 text-amber-600 dark:text-amber-400 shrink-0" />
     <div>
      <span className="font-bold text-amber-900 dark:text-amber-200">
       Demo Mode — Azure services are simulated.
      </span>
      <span className="ml-2 text-amber-800 dark:text-amber-300">
       All data ingestion, validation, quality scoring, and transformations execute locally in real time without requiring an active Azure subscription.
      </span>
     </div>
    </div>
    <button
     type="button"
     onClick={() => onNavigate('integrations')}
     className="px-3 py-1.5 rounded-lg bg-amber-600 text-white font-semibold hover:bg-amber-700 cursor-pointer"
    >
     Configure Azure Credentials →
    </button>
   </div>

   {/* Interactive 7-Component Architecture Flow Diagram */}
   <div className={`rounded-xl border p-5 overflow-x-auto ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
    <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-4">
     Interactive Medallion Data Flow (Click any Azure service block to inspect specifications)
    </div>
    <div className="min-w-[860px] grid grid-cols-7 gap-2.5">
     {AZURE_ARCHITECTURE_COMPONENTS.map((comp, idx) => {
      const isSel = comp.id === selectedComp.id;
      return (
       <div key={comp.id} className="flex items-center">
        <button
         type="button"
         onClick={() => setSelectedCompId(comp.id)}
         className={`w-full text-left p-3.5 rounded-xl border transition-all cursor-pointer ${
          isSel
           ? 'border-[#0078D4] bg-blue-50/60 dark:bg-blue-950/40 ring-2 ring-[#0078D4]/30 shadow-md'
           : darkMode
           ? 'border-slate-800 bg-slate-950/60 hover:border-slate-700'
           : 'border-slate-200 bg-slate-50/60 hover:border-blue-300'
         }`}
        >
         <div className="text-[10px] font-bold uppercase text-[#0078D4] dark:text-blue-400">
          {comp.layer}
         </div>
         <div className="font-bold text-xs mt-1">{comp.title}</div>
         <div className="text-[11px] text-slate-500 dark:text-slate-400 mt-1 line-clamp-2">
          {comp.azureProduct}
         </div>
         <div className="mt-3 pt-2 border-t border-slate-200/60 dark:border-slate-800">
          <StatusBadge status="Simulated" />
         </div>
        </button>
        {idx < AZURE_ARCHITECTURE_COMPONENTS.length - 1 && (
         <ArrowRight className="w-4 h-4 text-[#0078D4] shrink-0 mx-0.5" />
        )}
       </div>
      );
     })}
    </div>
   </div>

   {/* Selected Component Specification Inspector */}
   <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
    <div className={`lg:col-span-7 rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="flex items-center justify-between pb-3 border-b border-slate-100 dark:border-slate-800">
      <div>
       <span className="text-xs font-semibold text-[#0078D4]">{selectedComp.layer}</span>
       <h3 className="text-base font-bold mt-0.5">{selectedComp.title} — {selectedComp.azureProduct}</h3>
      </div>
      <StatusBadge status={selectedComp.integrationStatus} />
     </div>

     <div className="space-y-3 text-xs">
      <div>
       <div className="font-semibold text-slate-400 uppercase text-[10px]">Architectural Responsibility</div>
       <p className="mt-1 text-slate-700 dark:text-slate-200 leading-relaxed">{selectedComp.responsibility}</p>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-2">
       <div className={`p-3 rounded-lg border ${darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
        <div className="font-semibold text-slate-400 uppercase text-[10px]">Upstream Input</div>
        <div className="mt-1 font-medium">{selectedComp.inputSpec}</div>
       </div>
       <div className={`p-3 rounded-lg border ${darkMode ? 'bg-slate-950 border-slate-800' : 'bg-slate-50 border-slate-200'}`}>
        <div className="font-semibold text-slate-400 uppercase text-[10px]">Downstream Output</div>
        <div className="mt-1 font-medium font-mono text-[11px]">{selectedComp.outputSpec}</div>
       </div>
      </div>
     </div>
    </div>

    <div className={`lg:col-span-5 rounded-xl border p-5 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2.5">
      Infrastructure-as-Code / Service Configuration
     </div>
     <pre className="p-4 rounded-xl bg-slate-950 text-emerald-400 font-mono text-xs overflow-x-auto leading-relaxed">
      {selectedComp.configSnippet}
     </pre>
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 17. SERVICE INTEGRATIONS VIEW
// ============================================================================

const ServiceIntegrationsView: React.FC<{
 integrationMode: 'demo' | 'configurable';
 onToggleIntegrationMode: (mode: 'demo' | 'configurable') => void;
 darkMode: boolean;
}> = ({ integrationMode, onToggleIntegrationMode, darkMode }) => {
 const [storageAccount, setStorageAccount] = useState('azurepulsedatalake');
 const [containerBronze, setContainerBronze] = useState('bronze-raw');
 const [containerGold, setContainerGold] = useState('gold-curated');
 const [databricksUrl, setDatabricksUrl] = useState('https://adb-8492019482.14.azuredatabricks.net');
 const [synapseEndpoint, setSynapseEndpoint] = useState('azurepulse-synapse-ondemand.sql.azuresynapse.net');
 const [savedNotice, setSavedNotice] = useState(false);

 return (
  <div className="space-y-6">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Azure Service Integrations & Security Configuration</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Configure server-side Managed Identity bindings or operate in zero-credential Demo Simulation Mode.
     </p>
    </div>

    <div className="inline-flex rounded-xl p-1 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-xs">
     <button
      type="button"
      onClick={() => onToggleIntegrationMode('demo')}
      className={`px-3 py-1.5 rounded-lg font-semibold cursor-pointer ${
       integrationMode === 'demo' ? 'bg-white dark:bg-slate-900 text-amber-600 shadow-sm' : 'text-slate-500'
      }`}
     >
      Demo Mode
     </button>
     <button
      type="button"
      onClick={() => onToggleIntegrationMode('configurable')}
      className={`px-3 py-1.5 rounded-lg font-semibold cursor-pointer ${
       integrationMode === 'configurable' ? 'bg-white dark:bg-slate-900 text-[#0078D4] shadow-sm' : 'text-slate-500'
      }`}
     >
      Configurable Integration Mode
     </button>
    </div>
   </div>

   {/* Security Guardrail Notice */}
   <div className="p-4 rounded-xl border border-amber-300/80 bg-amber-50/80 dark:bg-amber-950/30 dark:border-amber-800 text-xs flex items-start gap-3">
    <Lock className="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
    <div>
     <div className="font-bold text-amber-900 dark:text-amber-200">
      Demo Mode — Azure services are simulated (No Live Cloud Credentials Verified)
     </div>
     <p className="text-amber-800 dark:text-amber-300 mt-0.5">
      Security Policy: Never enter or expose Azure Storage account keys, SAS tokens, client secrets, or personal access tokens in browser frontend code. Live Azure connections must use server-side <code>DefaultAzureCredential</code> (Managed Identity) via backend environment variables.
     </p>
    </div>
   </div>

   <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
    <div className={`lg:col-span-6 rounded-xl border p-5 space-y-4 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <h3 className="font-semibold text-sm pb-2 border-b border-slate-100 dark:border-slate-800">
      Non-Secret Resource Endpoint Configuration
     </h3>
     <div className="space-y-3 text-xs">
      <div>
       <label className="block font-medium mb-1">ADLS Gen2 Storage Account Name</label>
       <input
        type="text"
        value={storageAccount}
        onChange={e => setStorageAccount(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border font-mono ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>
      <div className="grid grid-cols-2 gap-3">
       <div>
        <label className="block font-medium mb-1">Bronze Raw Container</label>
        <input
         type="text"
         value={containerBronze}
         onChange={e => setContainerBronze(e.target.value)}
         className={`w-full px-3 py-2 rounded-lg border font-mono ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        />
       </div>
       <div>
        <label className="block font-medium mb-1">Gold Curated Container</label>
        <input
         type="text"
         value={containerGold}
         onChange={e => setContainerGold(e.target.value)}
         className={`w-full px-3 py-2 rounded-lg border font-mono ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
        />
       </div>
      </div>
      <div>
       <label className="block font-medium mb-1">Azure Databricks Workspace URL</label>
       <input
        type="text"
        value={databricksUrl}
        onChange={e => setDatabricksUrl(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border font-mono ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>
      <div>
       <label className="block font-medium mb-1">Azure Synapse Serverless SQL Endpoint</label>
       <input
        type="text"
        value={synapseEndpoint}
        onChange={e => setSynapseEndpoint(e.target.value)}
        className={`w-full px-3 py-2 rounded-lg border font-mono ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
       />
      </div>

      <button
       type="button"
       onClick={() => setSavedNotice(true)}
       className="px-4 py-2 rounded-lg bg-[#0078D4] hover:bg-blue-700 text-white font-semibold cursor-pointer"
      >
       Save Endpoint Metadata
      </button>
      {savedNotice && (
       <div className="text-emerald-600 dark:text-emerald-400 font-medium">
        Endpoint metadata saved to session configuration (Status: Simulated — awaiting server-side Managed Identity).
       </div>
      )}
     </div>
    </div>

    {/* Step-by-Step Production Azure Connection Guide */}
    <div className={`lg:col-span-6 rounded-xl border p-5 space-y-3 ${darkMode ? 'bg-slate-900/90 border-slate-800' : 'bg-white border-slate-200 shadow-sm'}`}>
     <h3 className="font-semibold text-sm pb-2 border-b border-slate-100 dark:border-slate-800">
      Production Azure Connection Runbook (Server-Side Managed Identity)
     </h3>
     <ol className="list-decimal list-inside space-y-2.5 text-xs text-slate-600 dark:text-slate-300 leading-relaxed">
      <li>
       <strong>Provision ADLS Gen2 with Hierarchical Namespace:</strong> Run <code>az storage account create --name {storageAccount} --enable-hierarchical-namespace true</code>.
      </li>
      <li>
       <strong>Assign RBAC Role to Backend Managed Identity:</strong> Grant <code>Storage Blob Data Contributor</code> on the storage account to your backend App Service / Container Apps Managed Identity.
      </li>
      <li>
       <strong>Set Server-Side Environment Variables:</strong> Configure <code>AZURE_STORAGE_ACCOUNT_NAME={storageAccount}</code> and authenticate on the Python backend using <code>DefaultAzureCredential()</code> from <code>azure-identity</code>.
      </li>
      <li>
       <strong>Mount Gold Delta Tables in Synapse:</strong> Create an external data source referencing <code>abfss://{containerGold}@{storageAccount}.dfs.core.windows.net/</code> using Workspace Managed Identity.
      </li>
     </ol>
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 18. EXECUTION LOGS & SETTINGS VIEWS
// ============================================================================

const ExecutionLogsView: React.FC<{
 logs: SystemLogEntry[];
 darkMode: boolean;
}> = ({ logs, darkMode }) => {
 const [serviceFilter, setServiceFilter] = useState<string>('ALL');
 const [levelFilter, setLevelFilter] = useState<string>('ALL');
 const [search, setSearch] = useState<string>('');

 const filteredLogs = useMemo(() => {
  return logs.filter(l => {
   if (serviceFilter !== 'ALL' && l.service !== serviceFilter) return false;
   if (levelFilter !== 'ALL' && l.level !== levelFilter) return false;
   if (search && !l.message.toLowerCase().includes(search.toLowerCase())) return false;
   return true;
  });
 }, [logs, serviceFilter, levelFilter, search]);

 const handleDownloadLogs = () => {
  const text = filteredLogs
   .map(l => `${l.timestamp} [${l.level}] [${l.service}] (Exec #${l.executionId}) ${l.message}`)
   .join('\n');
  triggerFileDownload(text, 'azurepulse_execution_logs.txt', 'text/plain;charset=utf-8;');
 };

 return (
  <div className="space-y-5">
   <div className="flex flex-wrap items-center justify-between gap-3">
    <div>
     <h1 className="text-xl font-bold tracking-tight">Technical Execution Logs</h1>
     <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
      Structured diagnostic telemetry across ADF Ingestion, ADLS Gen2, Databricks Spark, Quality Gate, and Synapse SQL.
     </p>
    </div>
    <button
     type="button"
     onClick={handleDownloadLogs}
     className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg text-xs font-semibold bg-[#0078D4] text-white cursor-pointer"
    >
     <Download className="w-3.5 h-3.5" />
     Download Log Stream (.txt)
    </button>
   </div>

   <div className={`p-3.5 rounded-xl border flex flex-wrap items-center gap-3 text-xs ${darkMode ? 'bg-slate-900 border-slate-800' : 'bg-white border-slate-200'}`}>
    <select
     value={serviceFilter}
     onChange={e => setServiceFilter(e.target.value)}
     className={`px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
    >
     <option value="ALL">All Services</option>
     <option value="ADF-Ingestion">ADF-Ingestion</option>
     <option value="ADLS-Gen2">ADLS-Gen2</option>
     <option value="Databricks-Spark">Databricks-Spark</option>
     <option value="Quality-Engine">Quality-Engine</option>
     <option value="Synapse-SQL">Synapse-SQL</option>
    </select>

    <select
     value={levelFilter}
     onChange={e => setLevelFilter(e.target.value)}
     className={`px-2.5 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
    >
     <option value="ALL">All Levels</option>
     <option value="INFO">INFO</option>
     <option value="WARN">WARN</option>
     <option value="ERROR">ERROR</option>
    </select>

    <input
     type="text"
     placeholder="Filter log messages..."
     value={search}
     onChange={e => setSearch(e.target.value)}
     className={`flex-1 min-w-[200px] px-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
    />
   </div>

   <div className="rounded-xl border border-slate-800 bg-slate-950 p-4 font-mono text-xs text-slate-200 space-y-1.5 max-h-[540px] overflow-y-auto">
    {filteredLogs.map(log => (
     <div key={log.id} className="flex items-start gap-2.5 py-1 border-b border-slate-900">
      <span className="text-slate-500 shrink-0 tabular-nums">{log.timestamp}</span>
      <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold shrink-0 ${
       log.level === 'ERROR'
        ? 'bg-red-500/20 text-red-400'
        : log.level === 'WARN'
        ? 'bg-amber-500/20 text-amber-400'
        : 'bg-blue-500/20 text-blue-400'
      }`}>
       {log.level}
      </span>
      <span className="text-indigo-400 shrink-0">[{log.service}]</span>
      <span className="text-slate-500 shrink-0">#{log.executionId}</span>
      <span className="text-slate-200 break-all">{log.message}</span>
     </div>
    ))}
   </div>
  </div>
 );
};

const SettingsView: React.FC<{
 darkMode: boolean;
 onToggleDarkMode: () => void;
 reducedMotion: boolean;
 onToggleReducedMotion: () => void;
 stageAnimationSpeedMs: number;
 onChangeStageSpeed: (ms: number) => void;
 onResetDemoPlatform: () => void;
}> = ({
 darkMode,
 onToggleDarkMode,
 reducedMotion,
 onToggleReducedMotion,
 stageAnimationSpeedMs,
 onChangeStageSpeed,
 onResetDemoPlatform
}) => {
 return (
  <div className="space-y-6 max-w-3xl">
   <div>
    <h1 className="text-xl font-bold tracking-tight">Platform Settings & Preferences</h1>
    <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
     Customize appearance, motion accessibility, pipeline simulation cadence, and demo state.
    </p>
   </div>

   <div className={`rounded-xl border divide-y text-xs ${darkMode ? 'bg-slate-900 border-slate-800 divide-slate-800' : 'bg-white border-slate-200 divide-slate-100 shadow-sm'}`}>
    <div className="p-4 flex items-center justify-between">
     <div>
      <div className="font-semibold text-sm">Color Theme</div>
      <p className="text-slate-500 mt-0.5">Switch between cool off-white Light theme and deep navy Dark theme.</p>
     </div>
     <button
      type="button"
      onClick={onToggleDarkMode}
      className="px-3.5 py-2 rounded-lg border border-slate-200 dark:border-slate-700 font-semibold cursor-pointer"
     >
      {darkMode ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
     </button>
    </div>

    <div className="p-4 flex items-center justify-between">
     <div>
      <div className="font-semibold text-sm">Reduced Motion Accessibility</div>
      <p className="text-slate-500 mt-0.5">Disable counter interpolation and stage transition delays.</p>
     </div>
     <input
      type="checkbox"
      checked={reducedMotion}
      onChange={onToggleReducedMotion}
      className="w-4 h-4 accent-[#0078D4]"
     />
    </div>

    <div className="p-4 flex items-center justify-between">
     <div>
      <div className="font-semibold text-sm">Pipeline Visual Stage Cadence</div>
      <p className="text-slate-500 mt-0.5">Presentation delay per pipeline node during interactive execution.</p>
     </div>
     <select
      value={stageAnimationSpeedMs}
      onChange={e => onChangeStageSpeed(Number(e.target.value))}
      className={`px-3 py-1.5 rounded-lg border ${darkMode ? 'bg-slate-800 border-slate-700' : 'bg-slate-50 border-slate-200'}`}
     >
      <option value={140}>Fast (140ms / stage)</option>
      <option value={260}>Standard (260ms / stage)</option>
      <option value={0}>Instant (0ms delay)</option>
     </select>
    </div>

    <div className="p-4 flex items-center justify-between">
     <div>
      <div className="font-semibold text-sm text-red-600 dark:text-red-400">Reset Platform Demo State</div>
      <p className="text-slate-500 mt-0.5">Restore default synthetic datasets, pipelines, and execution history.</p>
     </div>
     <button
      type="button"
      onClick={onResetDemoPlatform}
      className="px-3.5 py-2 rounded-lg bg-red-600 hover:bg-red-700 text-white font-semibold cursor-pointer"
     >
      Reset Demo Data
     </button>
    </div>
   </div>
  </div>
 );
};

// ============================================================================
// 19. MAIN APPLICATION SHELL & STATE ORCHESTRATOR
// ============================================================================

const DEFAULT_TRANSFORM_CONFIG: TransformationRuleConfig = {
 deduplicateExact: true,
 deduplicateBusinessKey: 'order_id',
 normalizeWhitespace: true,
 standardizeCategories: true,
 imputeMissingValues: true,
 dropInvalidDates: true,
 fixNegativeNumbers: true,
 deriveRevenue: true,
 deriveDateParts: true,
 deriveDealTier: true,
 filterEnabled: false,
 filterColumn: 'revenue',
 filterOperator: 'gt',
 filterValue: '500',
 aggregateGroupBy: 'category',
 aggregateMetric: 'sum'
};

function buildDatasetEntity(
 id: string,
 name: string,
 sourceName: string,
 sourceType: IngestionConfig['sourceType'],
 rawRows: DataRow[],
 execId: string,
 transformCfg: TransformationRuleConfig = DEFAULT_TRANSFORM_CONFIG
): DatasetEntity {
 const rawQuality = evaluateDataQuality(rawRows);
 const { curatedRows, quarantinedRows, steps } = executeTransformationPipeline(rawRows, transformCfg);
 const curatedQuality = evaluateDataQuality(curatedRows);
 const schema = inferDatasetSchema(rawRows);
 const curatedSchema = inferDatasetSchema(curatedRows);

 return {
  id,
  name,
  sourceName,
  sourceType,
  createdAt: '2026-10-21 09:15 IST',
  updatedAt: '2026-10-21 13:00 IST',
  rawRows,
  curatedRows,
  quarantinedRows,
  rawQuality,
  curatedQuality,
  schema,
  curatedSchema,
  status: 'Curated & Verified',
  latestExecutionId: execId,
  ingestionConfig: {
   sourceName,
   sourceType,
   datasetName: name,
   destinationLayer: 'raw_bronze',
   schemaStrategy: 'Infer & Evolve',
   duplicateHandling: 'Remove Exact & Key Duplicates',
   nullHandling: 'Impute Median / Defaults',
   dateParsingRule: 'ISO YYYY-MM-DD (Drop Invalid)',
   columnMappings: schema.map(c => ({
    sourceCol: c.name,
    targetCol: c.name,
    targetType: c.inferredType,
    required: true
   }))
  },
  transformationSteps: steps
 };
}

const DEFAULT_PIPELINE_NODES: PipelineNodeData[] = [
 { id: 'n-source', type: 'SOURCE', label: 'E-Commerce CSV', subtitle: 'Raw Source', azureService: 'CSV / JSON Source', x: 24, y: 130, config: {} },
 { id: 'n-ingest', type: 'INGEST', label: 'ADF Copy Activity', subtitle: 'Bronze Ingestion', azureService: 'Azure Data Factory', x: 194, y: 130, config: {} },
 { id: 'n-validate', type: 'VALIDATE', label: 'Quality Gate', subtitle: 'Schema & Null Check', azureService: 'Quality Engine', x: 364, y: 130, config: {} },
 { id: 'n-dedup', type: 'DEDUPLICATE', label: 'Key Deduplicator', subtitle: 'order_id Uniqueness', azureService: 'Databricks Spark', x: 534, y: 60, config: {} },
 { id: 'n-transform', type: 'TRANSFORM', label: 'Clean & Feature Eng', subtitle: 'Taxonomy & Revenue', azureService: 'Databricks PySpark', x: 534, y: 200, config: {} },
 { id: 'n-curated', type: 'CURATED_STORAGE', label: 'ADLS Gold Delta', subtitle: 'Curated Storage', azureService: 'ADLS Gen2 Gold', x: 724, y: 130, config: {} },
 { id: 'n-analytics', type: 'ANALYTICS', label: 'Synapse Serving', subtitle: 'Executive BI Views', azureService: 'Azure Synapse SQL', x: 904, y: 130, config: {} }
];

const DEFAULT_PIPELINE_EDGES: PipelineEdgeData[] = [
 { id: 'e-1', source: 'n-source', target: 'n-ingest' },
 { id: 'e-2', source: 'n-ingest', target: 'n-validate' },
 { id: 'e-3', source: 'n-validate', target: 'n-dedup' },
 { id: 'e-4', source: 'n-dedup', target: 'n-transform' },
 { id: 'e-5', source: 'n-transform', target: 'n-curated' },
 { id: 'e-6', source: 'n-curated', target: 'n-analytics' }
];

export default function App() {
 const [activePage, setActivePage] = useState<NavPageId>('dashboard');
 const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(false);
 const [darkMode, setDarkMode] = useState<boolean>(false);
 const [reducedMotion, setReducedMotion] = useState<boolean>(false);
 const [stageSpeedMs, setStageSpeedMs] = useState<number>(180);
 const [integrationMode, setIntegrationMode] = useState<'demo' | 'configurable'>('demo');
 const [dateRange, setDateRange] = useState<string>('ALL');
 const [globalSearchOpen, setGlobalSearchOpen] = useState<boolean>(false);
 const [globalSearchQuery, setGlobalSearchQuery] = useState<string>('');
 const [notificationsOpen, setNotificationsOpen] = useState<boolean>(false);
 const [toastMessage, setToastMessage] = useState<{ title: string; type: 'success' | 'error' | 'info' } | null>(null);

 const showToast = useCallback((title: string, type: 'success' | 'error' | 'info' = 'success') => {
  setToastMessage({ title, type });
  setTimeout(() => {
   setToastMessage(prev => (prev?.title === title ? null : prev));
  }, 3600);
 }, []);

 // Initialize Datasets
 const [datasets, setDatasets] = useState<DatasetEntity[]>(() => [
  buildDatasetEntity(
   'ds-ecommerce-2026',
   'Enterprise E-Commerce Orders 2026',
   'ecommerce_orders_2026.csv',
   'Synthetic Dataset',
   INITIAL_ECOMMERCE_RAW_ROWS,
   'EX-9042'
  ),
  buildDatasetEntity(
   'ds-marketplace-q3',
   'Global Cloud Marketplace Q3',
   'cloud_marketplace_q3.json',
   'JSON Upload',
   SECONDARY_MARKETPLACE_RAW_ROWS,
   'EX-9038'
  )
 ]);

 const [activeDatasetId, setActiveDatasetId] = useState<string>('ds-ecommerce-2026');

 const activeDataset = useMemo(
  () => datasets.find(d => d.id === activeDatasetId) || datasets[0],
  [datasets, activeDatasetId]
 );

 // Saved Pipelines
 const [savedPipelines, setSavedPipelines] = useState<PipelineDefinition[]>([
  {
   id: 'pl-enterprise-medallion',
   name: 'pl_ecommerce_medallion_gold_etl',
   description: 'Standard 7-stage Bronze-to-Gold deduplication, category normalization, and revenue derivation pipeline.',
   datasetId: 'ds-ecommerce-2026',
   nodes: DEFAULT_PIPELINE_NODES,
   edges: DEFAULT_PIPELINE_EDGES,
   transformConfig: DEFAULT_TRANSFORM_CONFIG,
   lastModified: '12:45 IST'
  },
  {
   id: 'pl-strategic-high-value',
   name: 'pl_strategic_orders_filtered_etl',
   description: 'Filters high-value enterprise orders (Revenue > $3,000) for executive board reporting.',
   datasetId: 'ds-ecommerce-2026',
   nodes: DEFAULT_PIPELINE_NODES,
   edges: DEFAULT_PIPELINE_EDGES,
   transformConfig: {
    ...DEFAULT_TRANSFORM_CONFIG,
    filterEnabled: true,
    filterColumn: 'revenue',
    filterOperator: 'gt',
    filterValue: '3000'
   },
   lastModified: '11:20 IST'
  }
 ]);

 const [activePipelineId, setActivePipelineId] = useState<string>('pl-enterprise-medallion');
 const activePipeline = useMemo(
  () => savedPipelines.find(p => p.id === activePipelineId) || savedPipelines[0],
  [savedPipelines, activePipelineId]
 );

 // Initial Pipeline Executions
 const [executions, setExecutions] = useState<PipelineExecutionRecord[]>([
  {
   executionId: 'EX-9042',
   pipelineId: 'pl-enterprise-medallion',
   pipelineName: 'pl_ecommerce_medallion_gold_etl',
   datasetId: 'ds-ecommerce-2026',
   datasetName: 'Enterprise E-Commerce Orders 2026',
   triggerType: 'Manual',
   startTime: '2026-10-21 12:58:10',
   completionTime: '2026-10-21 12:58:11',
   actualComputeMs: 4,
   totalDurationMs: 178,
   status: 'Succeeded',
   currentStageIndex: 6,
   inputRowCount: 52,
   outputRowCount: 46,
   retryCount: 0,
   stages: [
    { stageId: 'stg-1', stageName: 'Data Source', nodeType: 'SOURCE', azureService: 'CSV / JSON Source', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 12, logs: ['Read 52 records from ecommerce_orders_2026.csv'] },
    { stageId: 'stg-2', stageName: 'Ingestion', nodeType: 'INGEST', azureService: 'Azure Data Factory', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 28, logs: ['ADF Copy Activity ingested 52 records into Bronze landing'] },
    { stageId: 'stg-3', stageName: 'Raw Storage', nodeType: 'INGEST', azureService: 'ADLS Gen2 (Raw)', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 18, logs: ['Immutable raw snapshot saved to abfss://raw@azurepulsedatalake/bronze/'] },
    { stageId: 'stg-4', stageName: 'Data Validation', nodeType: 'VALIDATE', azureService: 'Quality Gate Engine', status: 'Succeeded', inputRows: 52, outputRows: 50, durationMs: 34, logs: ['Quarantined 2 rows with invalid calendar dates (ORD-1012, ORD-1028)'] },
    { stageId: 'stg-5', stageName: 'Transformation', nodeType: 'TRANSFORM', azureService: 'Azure Databricks', status: 'Succeeded', inputRows: 50, outputRows: 46, durationMs: 49, logs: ['Removed 4 duplicate records, standardized 11 category strings, imputed 4 nulls, derived revenue'] },
    { stageId: 'stg-6', stageName: 'Curated Storage', nodeType: 'CURATED_STORAGE', azureService: 'ADLS Gen2 (Gold)', status: 'Succeeded', inputRows: 46, outputRows: 46, durationMs: 21, logs: ['Committed 46 validated records to Gold Delta table (Quality Score: 100.0%)'] },
    { stageId: 'stg-7', stageName: 'Analytics', nodeType: 'ANALYTICS', azureService: 'Azure Synapse SQL', status: 'Succeeded', inputRows: 46, outputRows: 46, durationMs: 16, logs: ['Refreshed Synapse SQL views ($319,658.50 total curated revenue)'] }
   ]
  },
  {
   executionId: 'EX-9038',
   pipelineId: 'pl-enterprise-medallion',
   pipelineName: 'pl_ecommerce_medallion_gold_etl',
   datasetId: 'ds-marketplace-q3',
   datasetName: 'Global Cloud Marketplace Q3',
   triggerType: 'Scheduled ADF Trigger',
   startTime: '2026-10-21 11:30:00',
   completionTime: '2026-10-21 11:30:01',
   actualComputeMs: 3,
   totalDurationMs: 152,
   status: 'Succeeded',
   currentStageIndex: 6,
   inputRowCount: 13,
   outputRowCount: 11,
   retryCount: 0,
   stages: [
    { stageId: 'stg-1', stageName: 'Data Source', nodeType: 'SOURCE', azureService: 'CSV / JSON Source', status: 'Succeeded', inputRows: 13, outputRows: 13, durationMs: 10, logs: ['Read 13 JSON records from cloud_marketplace_q3.json'] },
    { stageId: 'stg-2', stageName: 'Ingestion', nodeType: 'INGEST', azureService: 'Azure Data Factory', status: 'Succeeded', inputRows: 13, outputRows: 13, durationMs: 22, logs: ['ADF ingested 13 records'] },
    { stageId: 'stg-3', stageName: 'Raw Storage', nodeType: 'INGEST', azureService: 'ADLS Gen2 (Raw)', status: 'Succeeded', inputRows: 13, outputRows: 13, durationMs: 16, logs: ['Persisted raw JSON snapshot'] },
    { stageId: 'stg-4', stageName: 'Data Validation', nodeType: 'VALIDATE', azureService: 'Quality Gate Engine', status: 'Succeeded', inputRows: 13, outputRows: 12, durationMs: 28, logs: ['Quarantined 1 invalid date record (MKT-2008)'] },
    { stageId: 'stg-5', stageName: 'Transformation', nodeType: 'TRANSFORM', azureService: 'Azure Databricks', status: 'Succeeded', inputRows: 12, outputRows: 11, durationMs: 42, logs: ['Removed 1 exact duplicate (MKT-2002), normalized categories & negative qty'] },
    { stageId: 'stg-6', stageName: 'Curated Storage', nodeType: 'CURATED_STORAGE', azureService: 'ADLS Gen2 (Gold)', status: 'Succeeded', inputRows: 11, outputRows: 11, durationMs: 19, logs: ['Committed 11 records to Gold layer'] },
    { stageId: 'stg-7', stageName: 'Analytics', nodeType: 'ANALYTICS', azureService: 'Azure Synapse SQL', status: 'Succeeded', inputRows: 11, outputRows: 11, durationMs: 15, logs: ['Updated Synapse views'] }
   ]
  },
  {
   executionId: 'EX-9031',
   pipelineId: 'pl-enterprise-medallion',
   pipelineName: 'pl_strict_schema_validation_run',
   datasetId: 'ds-ecommerce-2026',
   datasetName: 'Enterprise E-Commerce Orders 2026',
   triggerType: 'Manual',
   startTime: '2026-10-21 10:14:22',
   completionTime: '2026-10-21 10:14:23',
   actualComputeMs: 2,
   totalDurationMs: 96,
   status: 'Failed',
   currentStageIndex: 3,
   inputRowCount: 52,
   outputRowCount: 0,
   retryCount: 0,
   errorMessage: 'Strict Schema Gate Halted at Stage 04 (Data Validation): Detected 2 unparseable ISO dates (ORD-1012: "2026-13-45", ORD-1028: "INVALID_DATE") and 4 duplicate order_id keys.',
   stages: [
    { stageId: 'stg-1', stageName: 'Data Source', nodeType: 'SOURCE', azureService: 'CSV / JSON Source', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 11, logs: ['Read 52 records'] },
    { stageId: 'stg-2', stageName: 'Ingestion', nodeType: 'INGEST', azureService: 'Azure Data Factory', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 24, logs: ['Copied 52 rows to Raw landing'] },
    { stageId: 'stg-3', stageName: 'Raw Storage', nodeType: 'INGEST', azureService: 'ADLS Gen2 (Raw)', status: 'Succeeded', inputRows: 52, outputRows: 52, durationMs: 19, logs: ['Saved raw snapshot'] },
    { stageId: 'stg-4', stageName: 'Data Validation', nodeType: 'VALIDATE', azureService: 'Quality Gate Engine', status: 'Failed', inputRows: 52, outputRows: 0, durationMs: 42, errorMessage: 'Strict Schema Enforcement blocked 2 invalid dates & 4 duplicate keys.', logs: ['ERROR: Strict validation failed on ORD-1012 ("2026-13-45") and ORD-1028 ("INVALID_DATE").'] },
    { stageId: 'stg-5', stageName: 'Transformation', nodeType: 'TRANSFORM', azureService: 'Azure Databricks', status: 'Skipped', inputRows: 0, outputRows: 0, durationMs: 0, logs: ['Skipped due to upstream stage failure'] },
    { stageId: 'stg-6', stageName: 'Curated Storage', nodeType: 'CURATED_STORAGE', azureService: 'ADLS Gen2 (Gold)', status: 'Skipped', inputRows: 0, outputRows: 0, durationMs: 0, logs: ['Skipped due to upstream stage failure'] },
    { stageId: 'stg-7', stageName: 'Analytics', nodeType: 'ANALYTICS', azureService: 'Azure Synapse SQL', status: 'Skipped', inputRows: 0, outputRows: 0, durationMs: 0, logs: ['Skipped due to upstream stage failure'] }
   ]
  }
 ]);

 const [isExecuting, setIsExecuting] = useState<boolean>(false);

 // Alerts
 const [alerts, setAlerts] = useState<PlatformAlert[]>([
  {
   id: 'alt-1',
   type: 'Quality Threshold Violation',
   severity: 'warning',
   title: 'Raw Layer Anomalies Detected in E-Commerce Orders (91.1% Score)',
   description: 'Raw landing file contains 4 duplicate order_ids, 2 invalid calendar dates, 2 negative numeric values, and 11 unstandardized category labels. Remediated automatically in Curated Gold layer (100.0%).',
   datasetId: 'ds-ecommerce-2026',
   executionId: 'EX-9042',
   timestamp: '12:58 IST',
   acknowledged: false
  },
  {
   id: 'alt-2',
   type: 'Transformation Failure',
   severity: 'critical',
   title: 'Execution #EX-9031 Halted on Strict Date Validation',
   description: 'Strict schema enforcement blocked unparseable date strings ("2026-13-45"). Eligible for automatic quarantine retry in Pipeline Monitor.',
   datasetId: 'ds-ecommerce-2026',
   executionId: 'EX-9031',
   timestamp: '10:14 IST',
   acknowledged: false
  }
 ]);

 // Activity Feed
 const [activities, setActivities] = useState<ActivityEvent[]>([
  {
   id: 'act-1',
   timestamp: '2026-10-21 12:58:11',
   actor: 'Analyst (Manual Trigger)',
   category: 'Pipeline',
   title: 'Pipeline pl_ecommerce_medallion_gold_etl Succeeded (#EX-9042)',
   detail: 'Processed 52 raw records into 46 clean Gold records (100.0% quality score, $319,658.50 total revenue).',
   targetPage: 'monitor',
   status: 'success'
  },
  {
   id: 'act-2',
   timestamp: '2026-10-21 12:58:10',
   actor: 'Quality Gate Engine',
   category: 'Quality',
   title: 'Quarantined 6 Defective Raw Records During Transformation',
   detail: 'Removed 4 duplicate order_id records and 2 invalid date records; imputed 4 null cells.',
   targetPage: 'quality',
   status: 'warning'
  },
  {
   id: 'act-3',
   timestamp: '2026-10-21 11:30:01',
   actor: 'Azure Data Factory Trigger',
   category: 'Ingestion',
   title: 'Ingested Global Cloud Marketplace Q3 Dataset (#EX-9038)',
   detail: 'Loaded 13 JSON records into ADLS Gen2 Bronze and published 11 curated Gold records.',
   targetPage: 'explorer',
   status: 'success'
  }
 ]);

 // Saved Analytics Reports
 const [savedReports, setSavedReports] = useState<SavedAnalyticsReport[]>([
  {
   id: 'REP-401',
   title: 'Enterprise E-Commerce Q3–Q4 Revenue by Product Category',
   datasetId: 'ds-ecommerce-2026',
   datasetName: 'Enterprise E-Commerce Orders 2026',
   createdAt: '2026-10-21 12:59 IST',
   notes: 'Baseline executive summary derived from 46 validated Gold layer transactions across 5 standardized product categories.',
   filters: {
    dateRange: 'ALL',
    category: 'ALL',
    region: 'ALL',
    segment: 'ALL',
    groupBy: 'category',
    metric: 'sum'
   },
   summaryMetrics: {
    totalRevenue: 319658.5,
    totalOrders: 46,
    avgOrderValue: 6949.1,
    topCategory: 'Software Licenses',
    qualityScore: 100.0
   }
  }
 ]);

 // System Logs
 const [logs, setLogs] = useState<SystemLogEntry[]>([
  { id: 'lg-1', timestamp: '2026-10-21 12:58:10.104', service: 'ADF-Ingestion', level: 'INFO', executionId: 'EX-9042', message: 'Initialized Copy Activity from ecommerce_orders_2026.csv (52 rows).' },
  { id: 'lg-2', timestamp: '2026-10-21 12:58:10.142', service: 'ADLS-Gen2', level: 'INFO', executionId: 'EX-9042', message: 'Committed immutable raw file to abfss://raw@azurepulsedatalake.dfs.core.windows.net/bronze/ecommerce_orders_2026.csv' },
  { id: 'lg-3', timestamp: '2026-10-21 12:58:10.195', service: 'Quality-Engine', level: 'WARN', executionId: 'EX-9042', message: 'Raw Quality Score evaluated at 91.1% (4 duplicate keys, 2 invalid ISO dates, 11 unnormalized category strings).' },
  { id: 'lg-4', timestamp: '2026-10-21 12:58:10.260', service: 'Databricks-Spark', level: 'INFO', executionId: 'EX-9042', message: 'Executed PySpark/Pandas transformation DAG in 4ms compute: deduplicated 4 rows, quarantined 2 invalid dates, derived revenue & deal_size_tier.' },
  { id: 'lg-5', timestamp: '2026-10-21 12:58:10.310', service: 'Synapse-SQL', level: 'INFO', executionId: 'EX-9042', message: 'Refreshed Gold Serverless SQL view [gold].[vw_enterprise_sales] with 46 records ($319,658.50).' },
  { id: 'lg-6', timestamp: '2026-10-21 10:14:22.410', service: 'Quality-Engine', level: 'ERROR', executionId: 'EX-9031', message: 'Strict Schema Enforcement failed on row ORD-1012 (order_date="2026-13-45"). Dependent stages halted.' }
 ]);

 // Core Pipeline Execution Orchestrator (Animated Stage-by-Stage + Real Data Engine)
 const triggerPipelineExecution = useCallback(
  (
   targetDataset: DatasetEntity,
   pipelineDef: PipelineDefinition,
   triggerType: PipelineExecutionRecord['triggerType'] = 'Manual',
   simulateFailure: boolean = false
  ) => {
   if (isExecuting) return;
   setIsExecuting(true);

   const execId = `EX-${Math.floor(9045 + Math.random() * 900)}`;
   const nowStr = new Date().toTimeString().slice(0, 8);
   const startTimestamp = `2026-10-21 ${nowStr}`;

   const { curatedRows, quarantinedRows, steps, actualComputeMs } = executeTransformationPipeline(
    targetDataset.rawRows,
    pipelineDef.transformConfig
   );
   const rawCount = targetDataset.rawRows.length;
   const outCount = curatedRows.length;

   const stageNames = [
    { name: 'Data Source', type: 'SOURCE' as NodeType, service: 'CSV / JSON Source', inR: rawCount, outR: rawCount, log: `Loaded ${rawCount} raw records from ${targetDataset.sourceName}` },
    { name: 'Ingestion', type: 'INGEST' as NodeType, service: 'Azure Data Factory', inR: rawCount, outR: rawCount, log: `ADF Copy Activity transferred ${rawCount} records to Bronze landing` },
    { name: 'Raw Storage', type: 'INGEST' as NodeType, service: 'ADLS Gen2 (Raw)', inR: rawCount, outR: rawCount, log: `Persisted immutable raw snapshot in ADLS Gen2 Bronze container` },
    { name: 'Data Validation', type: 'VALIDATE' as NodeType, service: 'Quality Gate Engine', inR: rawCount, outR: simulateFailure ? 0 : rawCount - 2, log: simulateFailure ? `ERROR: Strict Schema Gate halted execution due to unparseable date values` : `Validated schema & quarantined unparseable date rows` },
    { name: 'Transformation', type: 'TRANSFORM' as NodeType, service: 'Azure Databricks', inR: Math.max(0, rawCount - 2), outR: outCount, log: `Executed deduplication, whitespace/category normalization, null imputation & feature engineering` },
    { name: 'Curated Storage', type: 'CURATED_STORAGE' as NodeType, service: 'ADLS Gen2 (Gold)', inR: outCount, outR: outCount, log: `Committed ${outCount} clean records to Gold Delta table (Quality Score: 100.0%)` },
    { name: 'Analytics', type: 'ANALYTICS' as NodeType, service: 'Azure Synapse SQL', inR: outCount, outR: outCount, log: `Refreshed Synapse Serverless SQL views and executive KPI cards` }
   ];

   const initialStages: StageExecutionRecord[] = stageNames.map((s, i) => ({
    stageId: `stg-${i + 1}`,
    stageName: s.name,
    nodeType: s.type,
    azureService: s.service,
    status: i === 0 ? 'Running' : 'Pending',
    inputRows: s.inR,
    outputRows: s.outR,
    durationMs: 0,
    logs: [`Starting ${s.name}...`]
   }));

   const newExec: PipelineExecutionRecord = {
    executionId: execId,
    pipelineId: pipelineDef.id,
    pipelineName: pipelineDef.name,
    datasetId: targetDataset.id,
    datasetName: targetDataset.name,
    triggerType,
    startTime: startTimestamp,
    completionTime: 'In Progress...',
    actualComputeMs,
    totalDurationMs: 0,
    status: 'Running',
    currentStageIndex: 0,
    inputRowCount: rawCount,
    outputRowCount: outCount,
    retryCount: triggerType === 'Auto-Retry' ? 1 : 0,
    stages: initialStages
   };

   setExecutions(prev => [newExec, ...prev]);

   const delay = reducedMotion ? 0 : stageSpeedMs;

   stageNames.forEach((stgMeta, idx) => {
    setTimeout(() => {
     setExecutions(prev =>
      prev.map(ex => {
       if (ex.executionId !== execId) return ex;
       if (simulateFailure && idx > 3) return ex;

       const updatedStages = ex.stages.map((st, sIdx) => {
        if (sIdx < idx) return st;
        if (sIdx === idx) {
         if (simulateFailure && idx === 3) {
          return {
           ...st,
           status: 'Failed' as StageStatus,
           outputRows: 0,
           durationMs: 36,
           errorMessage: 'Strict Schema Enforcement blocked invalid ISO date strings.',
           logs: [stgMeta.log]
          };
         }
         return {
          ...st,
          status: 'Succeeded' as StageStatus,
          durationMs: 14 + idx * 6,
          logs: [stgMeta.log]
         };
        }
        if (simulateFailure && idx === 3 && sIdx > 3) {
         return {
          ...st,
          status: 'Skipped' as StageStatus,
          inputRows: 0,
          outputRows: 0,
          logs: ['Skipped due to upstream validation failure']
         };
        }
        if (sIdx === idx + 1) {
         return { ...st, status: 'Running' as StageStatus };
        }
        return st;
       });

       const isFinalStep = (simulateFailure && idx === 3) || idx === stageNames.length - 1;
       const totalDur = updatedStages.reduce((a, b) => a + b.durationMs, 0);

       return {
        ...ex,
        currentStageIndex: idx,
        totalDurationMs: totalDur,
        completionTime: isFinalStep ? `2026-10-21 ${new Date().toTimeString().slice(0, 8)}` : 'Running...',
        status: isFinalStep ? (simulateFailure ? 'Failed' : 'Succeeded') : 'Running',
        outputRowCount: simulateFailure ? 0 : outCount,
        errorMessage: simulateFailure ? 'Stage 04 (Data Validation) failed strict date & schema check. Click Retry to run with quarantine remediation.' : undefined,
        stages: updatedStages
       };
      })
     );

     // Finalize state updates when pipeline finishes
     if (simulateFailure && idx === 3) {
      setIsExecuting(false);
      setAlerts(prev => [
       {
        id: `alt-${Date.now()}`,
        type: 'Schema Mismatch',
        severity: 'critical',
        title: `Pipeline Execution #${execId} Failed at Validation Stage`,
        description: `Strict schema check halted execution on ${targetDataset.name}. Click Retry in Pipeline Monitor to quarantine invalid rows.`,
        datasetId: targetDataset.id,
        executionId: execId,
        timestamp: 'Just now',
        acknowledged: false
       },
       ...prev
      ]);
      setLogs(prev => [
       {
        id: `lg-${Date.now()}`,
        timestamp: `2026-10-21 ${new Date().toTimeString().slice(0, 8)}`,
        service: 'Quality-Engine',
        level: 'ERROR',
        executionId: execId,
        message: `Execution #${execId} halted at Data Validation stage. Downstream stages skipped.`
       },
       ...prev
      ]);
      showToast(`Execution #${execId} halted at Data Validation stage`, 'error');
     } else if (!simulateFailure && idx === stageNames.length - 1) {
      const newCuratedQuality = evaluateDataQuality(curatedRows);
      const newCuratedSchema = inferDatasetSchema(curatedRows);
      setDatasets(prev =>
       prev.map(d =>
        d.id === targetDataset.id
         ? {
           ...d,
           curatedRows,
           quarantinedRows,
           curatedQuality: newCuratedQuality,
           curatedSchema: newCuratedSchema,
           transformationSteps: steps,
           latestExecutionId: execId,
           updatedAt: 'Just now',
           status: 'Curated & Verified'
          }
         : d
       )
      );
      setActivities(prev => [
       {
        id: `act-${Date.now()}`,
        timestamp: `2026-10-21 ${new Date().toTimeString().slice(0, 8)}`,
        actor: `Pipeline Engine (${triggerType})`,
        category: 'Pipeline',
        title: `Execution #${execId} Succeeded on ${targetDataset.name}`,
        detail: `Transformed ${rawCount} raw rows → ${outCount} curated Gold rows (Quality Score: ${newCuratedQuality.overallScore}%).`,
        targetPage: 'monitor',
        status: 'success'
       },
       ...prev
      ]);
      setLogs(prev => [
       {
        id: `lg-${Date.now()}`,
        timestamp: `2026-10-21 ${new Date().toTimeString().slice(0, 8)}`,
        service: 'Databricks-Spark',
        level: 'INFO',
        executionId: execId,
        message: `Pipeline ${pipelineDef.name} completed all 7 stages (${rawCount} raw -> ${outCount} curated rows in ${actualComputeMs}ms compute).`
       },
       ...prev
      ]);
      setIsExecuting(false);
      showToast(`Pipeline #${execId} succeeded (${outCount} Gold rows updated)`, 'success');
     }
    }, (idx + 1) * Math.max(20, delay));
   });
  },
  [isExecuting, reducedMotion, stageSpeedMs, showToast]
 );

 // Retry a failed execution without duplicating output
 const handleRetryFailedExecution = useCallback(
  (failedExecId?: string) => {
   const targetExec = failedExecId
    ? executions.find(e => e.executionId === failedExecId)
    : executions.find(e => e.status === 'Failed');
   const ds = targetExec
    ? datasets.find(d => d.id === targetExec.datasetId) || activeDataset
    : activeDataset;

   if (targetExec) {
    // Mark the old failed execution as retried in logs and run clean quarantine remediation
    setExecutions(prev =>
     prev.map(e =>
      e.executionId === targetExec.executionId
       ? { ...e, retryCount: e.retryCount + 1 }
       : e
     )
    );
   }
   triggerPipelineExecution(ds, activePipeline, 'Auto-Retry', false);
  },
  [executions, datasets, activeDataset, activePipeline, triggerPipelineExecution]
 );

 // Ingest New Uploaded Dataset
 const handleIngestNewDataset = useCallback(
  (rawRows: DataRow[], config: IngestionConfig, runImmediately: boolean) => {
   const newId = `ds-${Date.now().toString().slice(-5)}`;
   const execId = `EX-${Math.floor(9100 + Math.random() * 800)}`;
   const newDs = buildDatasetEntity(
    newId,
    config.datasetName,
    config.sourceName,
    config.sourceType,
    rawRows,
    execId,
    activePipeline.transformConfig
   );
   newDs.ingestionConfig = config;

   setDatasets(prev => [newDs, ...prev]);
   setActiveDatasetId(newId);

   setActivities(prev => [
    {
     id: `act-${Date.now()}`,
     timestamp: `2026-10-21 ${new Date().toTimeString().slice(0, 8)}`,
     actor: 'Data Ingestion Module',
     category: 'Ingestion',
     title: `Ingested New Dataset "${config.datasetName}" (${rawRows.length} rows)`,
     detail: `Source: ${config.sourceName} (${config.sourceType}) → Destination: ${config.destinationLayer}.`,
     targetPage: 'explorer',
     status: 'success'
    },
    ...prev
   ]);

   showToast(`Ingested "${config.datasetName}" (${rawRows.length} raw rows)`, 'success');
   if (runImmediately) {
    setActivePage('dashboard');
    setTimeout(() => {
     triggerPipelineExecution(newDs, activePipeline, 'File Upload', false);
    }, 120);
   } else {
    setActivePage('explorer');
   }
  },
  [activePipeline, triggerPipelineExecution, showToast]
 );

 // Sidebar navigation structure (All 15 required sections & pages)
 const navSections: { section: string; items: { id: NavPageId; label: string; icon: any; badge?: string }[] }[] = [
  {
   section: 'OVERVIEW',
   items: [
    { id: 'dashboard', label: 'Overview Dashboard', icon: LayoutDashboard },
    { id: 'activity', label: 'Activity Feed', icon: Activity, badge: String(activities.length) }
   ]
  },
  {
   section: 'DATA ENGINEERING',
   items: [
    { id: 'sources', label: 'Data Sources', icon: HardDrive, badge: String(datasets.length) },
    { id: 'ingestion', label: 'Data Ingestion', icon: Upload },
    { id: 'builder', label: 'Pipeline Builder', icon: Workflow },
    { id: 'monitor', label: 'Pipeline Monitor', icon: Cpu, badge: String(executions.length) },
    { id: 'quality', label: 'Data Quality', icon: ShieldCheck, badge: `${activeDataset.curatedQuality.overallScore}%` },
    { id: 'explorer', label: 'Dataset Explorer', icon: TableIcon }
   ]
  },
  {
   section: 'ANALYTICS',
   items: [
    { id: 'analytics', label: 'Analytics Dashboard', icon: BarChart3 },
    { id: 'reports', label: 'Reports', icon: FileText, badge: String(savedReports.length) },
    { id: 'catalog', label: 'Data Catalog', icon: BookOpen }
   ]
  },
  {
   section: 'CLOUD OPERATIONS',
   items: [
    { id: 'architecture', label: 'Azure Architecture', icon: Network },
    { id: 'integrations', label: 'Service Integrations', icon: Cloud },
    { id: 'logs', label: 'Execution Logs', icon: Terminal },
    { id: 'settings', label: 'Settings', icon: SettingsIcon }
   ]
  }
 ];

 const currentNavMeta = useMemo(() => {
  for (const sec of navSections) {
   const found = sec.items.find(i => i.id === activePage);
   if (found) return { section: sec.section, label: found.label };
  }
  return { section: 'OVERVIEW', label: 'Overview Dashboard' };
 }, [activePage, navSections]);

 // Global search results
 const searchResults = useMemo(() => {
  const q = globalSearchQuery.trim().toLowerCase();
  const allPages = navSections.flatMap(s => s.items.map(i => ({ ...i, section: s.section })));
  if (!q) return allPages.slice(0, 8);
  return allPages.filter(p => p.label.toLowerCase().includes(q) || p.section.toLowerCase().includes(q));
 }, [globalSearchQuery]);

 const unacknowledgedAlerts = alerts.filter(a => !a.acknowledged);

 return (
  <div className={`${darkMode ? 'dark bg-[#0B1120] text-slate-100' : 'bg-[#F8FAFC] text-[#0F172A]'} min-h-screen flex font-sans antialiased transition-colors duration-200`}>
   {/* Collapsible Left Sidebar */}
   <aside
    className={`${
     sidebarCollapsed ? 'w-16' : 'w-64'
    } shrink-0 border-r transition-all duration-200 flex flex-col justify-between sticky top-0 h-screen z-30 ${
     darkMode ? 'bg-[#0F172A] border-slate-800/90' : 'bg-white border-slate-200/90'
    }`}
   >
    {/* Brand Header */}
    <div>
     <div className="h-16 px-4 border-b border-slate-100 dark:border-slate-800/80 flex items-center justify-between">
      <div
       onClick={() => setActivePage('dashboard')}
       className="flex items-center gap-2.5 cursor-pointer overflow-hidden"
      >
       <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[#0078D4] to-indigo-600 flex items-center justify-center text-white font-bold shadow-sm shrink-0">
        <Zap className="w-4 h-4" />
       </div>
       {!sidebarCollapsed && (
        <div className="leading-tight">
         <div className="font-bold text-sm tracking-tight flex items-center gap-1.5">
          <span>AzurePulse</span>
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-[#0078D4]/10 text-[#0078D4] dark:text-blue-400 font-semibold">
           v2.4
          </span>
         </div>
         <div className="text-[10px] text-slate-400 truncate">Cloud Data & Analytics</div>
        </div>
       )}
      </div>
      <button
       type="button"
       onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
       title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
       className="p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-400 cursor-pointer"
      >
       {sidebarCollapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
      </button>
     </div>

     {/* Navigation Sections */}
     <nav className="p-2.5 space-y-4 overflow-y-auto max-h-[calc(100vh-120px)]">
      {navSections.map(sec => (
       <div key={sec.section}>
        {!sidebarCollapsed && (
         <div className="px-2.5 mb-1.5 text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500">
          {sec.section}
         </div>
        )}
        <div className="space-y-0.5">
         {sec.items.map(item => {
          const IconComp = item.icon;
          const isActive = activePage === item.id;
          return (
           <button
            key={item.id}
            type="button"
            title={sidebarCollapsed ? item.label : undefined}
            onClick={() => setActivePage(item.id)}
            className={`w-full flex items-center justify-between px-2.5 py-2 rounded-lg text-xs font-medium transition-all duration-150 cursor-pointer ${
             isActive
              ? 'bg-gradient-to-r from-[#0078D4] to-blue-600 text-white shadow-sm'
              : darkMode
              ? 'text-slate-400 hover:bg-slate-800/70 hover:text-slate-100'
              : 'text-slate-600 hover:bg-slate-100/80 hover:text-slate-900'
            }`}
           >
            <div className="flex items-center gap-2.5 truncate">
             <IconComp className={`w-4 h-4 shrink-0 ${isActive ? 'text-white' : 'text-slate-400'}`} />
             {!sidebarCollapsed && <span className="truncate">{item.label}</span>}
            </div>
            {!sidebarCollapsed && item.badge && (
             <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-mono tabular-nums ${
              isActive
               ? 'bg-white/20 text-white'
               : 'bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400'
             }`}>
              {item.badge}
             </span>
            )}
           </button>
          );
         })}
        </div>
       </div>
      ))}
     </nav>
    </div>

    {/* Bottom Status Footer */}
    <div className="p-3 border-t border-slate-100 dark:border-slate-800/80 text-[11px]">
     {!sidebarCollapsed ? (
      <div className="flex items-center justify-between text-slate-400">
       <span className="flex items-center gap-1.5">
        <span className="w-2 h-2 rounded-full bg-amber-500" />
        <span>Demo Simulation</span>
       </span>
       <span className="font-mono">ADLS Gen2</span>
      </div>
     ) : (
      <div className="flex justify-center">
       <span className="w-2 h-2 rounded-full bg-amber-500" title="Demo Mode — Azure services are simulated" />
      </div>
     )}
    </div>
   </aside>

   {/* Main Content Shell */}
   <div className="flex-1 flex flex-col min-w-0">
    {/* Top Header */}
    <header className={`h-16 px-6 border-b sticky top-0 z-20 flex items-center justify-between gap-4 backdrop-blur-md ${
     darkMode ? 'bg-[#0F172A]/90 border-slate-800' : 'bg-white/90 border-slate-200/90'
    }`}>
     {/* Breadcrumbs */}
     <div className="flex items-center gap-2 text-xs">
      <span className="text-slate-400 font-medium hidden sm:inline">AzurePulse</span>
      <ChevronRight className="w-3.5 h-3.5 text-slate-400 hidden sm:inline" />
      <span className="text-slate-400 font-medium hidden md:inline">{currentNavMeta.section}</span>
      <ChevronRight className="w-3.5 h-3.5 text-slate-400 hidden md:inline" />
      <span className="font-semibold text-[#0078D4] dark:text-blue-400">{currentNavMeta.label}</span>
     </div>

     {/* Right Header Controls: Global Search, Demo Mode Pill, Notifications, Theme Switcher, User Profile */}
     <div className="flex items-center gap-2.5">
      {/* Global Search Trigger */}
      <button
       type="button"
       onClick={() => setGlobalSearchOpen(true)}
       className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border text-xs transition-colors cursor-pointer ${
        darkMode
         ? 'bg-slate-800/90 border-slate-700 text-slate-300 hover:border-slate-600'
         : 'bg-slate-50 border-slate-200 text-slate-500 hover:border-slate-300'
       }`}
      >
       <Search className="w-3.5 h-3.5" />
       <span className="hidden sm:inline">Search views, datasets, pipelines...</span>
       <kbd className="hidden md:inline px-1.5 py-0.5 text-[10px] font-mono rounded bg-slate-200/70 dark:bg-slate-700 text-slate-600 dark:text-slate-300">
        ⌘K
       </kbd>
      </button>

      {/* Honest Demo Mode Badge */}
      <button
       type="button"
       onClick={() => setActivePage('architecture')}
       className="hidden xl:inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium bg-amber-500/10 text-amber-700 dark:text-amber-300 border border-amber-500/20 cursor-pointer"
      >
       <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
       Demo Mode — Azure services are simulated
      </button>

      {/* Notifications */}
      <div className="relative">
       <button
        type="button"
        onClick={() => setNotificationsOpen(!notificationsOpen)}
        className={`p-2 rounded-lg border relative cursor-pointer ${
         darkMode ? 'border-slate-700 bg-slate-800 text-slate-200' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
        }`}
        aria-label="Notifications"
       >
        <Bell className="w-4 h-4" />
        {unacknowledgedAlerts.length > 0 && (
         <span className="absolute -top-1 -right-1 w-4 h-4 rounded-full bg-red-500 text-white text-[10px] font-bold flex items-center justify-center">
          {unacknowledgedAlerts.length}
         </span>
        )}
       </button>

       {notificationsOpen && (
        <div className={`absolute right-0 mt-2 w-80 rounded-xl border p-3.5 shadow-xl z-50 text-xs ${
         darkMode ? 'bg-slate-900 border-slate-700 text-slate-100' : 'bg-white border-slate-200 text-slate-900'
        }`}>
         <div className="flex items-center justify-between pb-2 mb-2 border-b border-slate-100 dark:border-slate-800">
          <span className="font-bold">Platform Notifications</span>
          <button
           type="button"
           onClick={() => setNotificationsOpen(false)}
           className="text-slate-400 hover:text-slate-600 cursor-pointer"
          >
           <X className="w-3.5 h-3.5" />
          </button>
         </div>
         {unacknowledgedAlerts.length === 0 ? (
          <div className="py-4 text-center text-slate-400">No unread alerts</div>
         ) : (
          <div className="space-y-2 max-h-60 overflow-y-auto">
           {unacknowledgedAlerts.map(a => (
            <div key={a.id} className="p-2.5 rounded-lg bg-slate-50 dark:bg-slate-800/70">
             <div className="font-semibold">{a.title}</div>
             <p className="text-[11px] text-slate-500 mt-0.5">{a.description}</p>
            </div>
           ))}
          </div>
         )}
        </div>
       )}
      </div>

      {/* Theme Switcher */}
      <button
       type="button"
       onClick={() => setDarkMode(!darkMode)}
       title={darkMode ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
       className={`p-2 rounded-lg border cursor-pointer transition-colors ${
        darkMode ? 'border-slate-700 bg-slate-800 text-amber-400 hover:bg-slate-700' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
       }`}
      >
       {darkMode ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
      </button>

      {/* User Profile */}
      <div className="flex items-center gap-2 pl-2 border-l border-slate-200 dark:border-slate-800">
       <div className="w-8 h-8 rounded-full bg-gradient-to-tr from-[#0078D4] to-indigo-600 text-white text-xs font-bold flex items-center justify-center">
        DA
       </div>
       <div className="hidden lg:block text-left leading-tight">
        <div className="text-xs font-semibold">Data Analyst</div>
        <div className="text-[10px] text-slate-400">Azure Data Eng</div>
       </div>
      </div>
     </div>
    </header>

    {/* Toast Notification */}
    {toastMessage && (
     <div className="fixed bottom-5 right-5 z-50 flex items-center gap-2.5 px-4 py-3 rounded-xl shadow-lg border text-xs font-semibold bg-slate-900 text-white border-slate-700 transition-all">
      {toastMessage.type === 'error' ? (
       <AlertCircle className="w-4 h-4 text-red-400" />
      ) : (
       <CheckCircle2 className="w-4 h-4 text-emerald-400" />
      )}
      <span>{toastMessage.title}</span>
     </div>
    )}

    {/* Global Search Modal */}
    {globalSearchOpen && (
     <div
      onClick={() => setGlobalSearchOpen(false)}
      className="fixed inset-0 z-50 bg-slate-950/50 backdrop-blur-sm flex items-start justify-center pt-20 p-4"
     >
      <div
       onClick={e => e.stopPropagation()}
       className={`w-full max-w-lg rounded-2xl border p-4 shadow-2xl ${
        darkMode ? 'bg-slate-900 border-slate-700 text-slate-100' : 'bg-white border-slate-200 text-slate-900'
       }`}
      >
       <div className="flex items-center gap-2 pb-3 border-b border-slate-100 dark:border-slate-800">
        <Search className="w-4 h-4 text-[#0078D4]" />
        <input
         type="text"
         autoFocus
         placeholder="Jump to any module, dataset, or pipeline view..."
         value={globalSearchQuery}
         onChange={e => setGlobalSearchQuery(e.target.value)}
         className="w-full text-xs outline-none bg-transparent"
        />
        <button
         type="button"
         onClick={() => setGlobalSearchOpen(false)}
         className="text-xs text-slate-400 hover:text-slate-600 cursor-pointer"
        >
         ESC
        </button>
       </div>
       <div className="mt-2 divide-y divide-slate-100 dark:divide-slate-800 max-h-72 overflow-y-auto">
        {searchResults.map(res => {
         const IconComp = res.icon;
         return (
          <button
           key={res.id}
           type="button"
           onClick={() => {
            setActivePage(res.id);
            setGlobalSearchOpen(false);
            setGlobalSearchQuery('');
           }}
           className="w-full py-2.5 px-3 flex items-center justify-between text-xs hover:bg-blue-500/10 rounded-lg cursor-pointer"
          >
           <span className="flex items-center gap-2.5 font-medium">
            <IconComp className="w-4 h-4 text-[#0078D4]" />
            {res.label}
           </span>
           <span className="text-[10px] uppercase text-slate-400">{res.section}</span>
          </button>
         );
        })}
       </div>
      </div>
     </div>
    )}

    {/* Active Route View Content with Smooth Fade/Rise Transition */}
    <main key={activePage} className="flex-1 p-5 sm:p-6 max-w-[1600px] w-full mx-auto transition-all duration-200">
     {activePage === 'dashboard' && (
      <OverviewDashboardView
       datasets={datasets}
       activeDatasetId={activeDataset.id}
       onSelectDataset={setActiveDatasetId}
       dateRange={dateRange}
       onSelectDateRange={setDateRange}
       executions={executions}
       isExecuting={isExecuting}
       onRunPipeline={(simFail = false) =>
        triggerPipelineExecution(activeDataset, activePipeline, 'Manual', simFail)
       }
       onRetryPipeline={() => handleRetryFailedExecution()}
       alerts={alerts}
       onAcknowledgeAlert={id =>
        setAlerts(prev => prev.map(a => (a.id === id ? { ...a, acknowledged: true } : a)))
       }
       onNavigate={setActivePage}
       reducedMotion={reducedMotion}
       darkMode={darkMode}
      />
     )}

     {activePage === 'activity' && (
      <ActivityFeedView
       activities={activities}
       onNavigate={setActivePage}
       darkMode={darkMode}
      />
     )}

     {activePage === 'sources' && (
      <DataSourcesView
       datasets={datasets}
       activeDatasetId={activeDataset.id}
       onSelectDataset={setActiveDatasetId}
       onNavigate={setActivePage}
       onConnectSampleDatabase={() => {
        const replicaRows = parseUploadedCSV(SAMPLE_UPLOAD_CSV_TEXT);
        handleIngestNewDataset(
         replicaRows,
         {
          sourceName: 'AzureSQL_RegionalPartnerOrders_Replica',
          sourceType: 'SQL Read Replica',
          datasetName: 'SQL Replica — Partner Orders 2026',
          destinationLayer: 'raw_bronze',
          schemaStrategy: 'Infer & Evolve',
          duplicateHandling: 'Remove Exact & Key Duplicates',
          nullHandling: 'Impute Median / Defaults',
          dateParsingRule: 'ISO YYYY-MM-DD (Drop Invalid)',
          columnMappings: []
         },
         true
        );
       }}
       darkMode={darkMode}
      />
     )}

     {activePage === 'ingestion' && (
      <DataIngestionView
       onIngestNewDataset={handleIngestNewDataset}
       darkMode={darkMode}
      />
     )}

     {activePage === 'builder' && (
      <PipelineBuilderView
       pipeline={activePipeline}
       savedPipelines={savedPipelines}
       onSelectSavedPipeline={setActivePipelineId}
       onSavePipeline={updated => {
        setSavedPipelines(prev => prev.map(p => (p.id === updated.id ? updated : p)));
        showToast(`Saved pipeline "${updated.name}"`, 'success');
       }}
       onRunCustomPipeline={(updated, simFail) => {
        setSavedPipelines(prev => prev.map(p => (p.id === updated.id ? updated : p)));
        triggerPipelineExecution(activeDataset, updated, 'Manual', simFail);
       }}
       activeDataset={activeDataset}
       isExecuting={isExecuting}
       darkMode={darkMode}
      />
     )}

     {activePage === 'monitor' && (
      <PipelineMonitorView
       executions={executions}
       isExecuting={isExecuting}
       onRunPipeline={(simFail = false) =>
        triggerPipelineExecution(activeDataset, activePipeline, 'Manual', simFail)
       }
       onRetryExecution={execId => handleRetryFailedExecution(execId)}
       darkMode={darkMode}
      />
     )}

     {activePage === 'quality' && (
      <DataQualityView
       dataset={activeDataset}
       onRemediateAndRun={() =>
        triggerPipelineExecution(activeDataset, activePipeline, 'Manual', false)
       }
       darkMode={darkMode}
      />
     )}

     {activePage === 'explorer' && (
      <DatasetExplorerView
       datasets={datasets}
       activeDatasetId={activeDataset.id}
       onSelectDataset={setActiveDatasetId}
       darkMode={darkMode}
      />
     )}

     {activePage === 'analytics' && (
      <AnalyticsDashboardView
       datasets={datasets}
       activeDatasetId={activeDataset.id}
       onSelectDataset={setActiveDatasetId}
       onSaveReportFromAnalytics={rep => {
        setSavedReports(prev => [rep, ...prev]);
        showToast(`Saved report "${rep.title}"`, 'success');
       }}
       onNavigate={setActivePage}
       darkMode={darkMode}
      />
     )}

     {activePage === 'reports' && (
      <ReportsView
       reports={savedReports}
       datasets={datasets}
       onCreateReport={rep => {
        setSavedReports(prev => [rep, ...prev]);
        showToast(`Created report "${rep.title}"`, 'success');
       }}
       darkMode={darkMode}
      />
     )}

     {activePage === 'catalog' && (
      <DataCatalogView
       datasets={datasets}
       onNavigate={setActivePage}
       darkMode={darkMode}
      />
     )}

     {activePage === 'architecture' && (
      <AzureArchitectureView
       integrationMode={integrationMode}
       onToggleIntegrationMode={setIntegrationMode}
       onNavigate={setActivePage}
       darkMode={darkMode}
      />
     )}

     {activePage === 'integrations' && (
      <ServiceIntegrationsView
       integrationMode={integrationMode}
       onToggleIntegrationMode={setIntegrationMode}
       darkMode={darkMode}
      />
     )}

     {activePage === 'logs' && (
      <ExecutionLogsView logs={logs} darkMode={darkMode} />
     )}

     {activePage === 'settings' && (
      <SettingsView
       darkMode={darkMode}
       onToggleDarkMode={() => setDarkMode(!darkMode)}
       reducedMotion={reducedMotion}
       onToggleReducedMotion={() => setReducedMotion(!reducedMotion)}
       stageAnimationSpeedMs={stageSpeedMs}
       onChangeStageSpeed={setStageSpeedMs}
       onResetDemoPlatform={() => {
        setDatasets([
         buildDatasetEntity(
          'ds-ecommerce-2026',
          'Enterprise E-Commerce Orders 2026',
          'ecommerce_orders_2026.csv',
          'Synthetic Dataset',
          INITIAL_ECOMMERCE_RAW_ROWS,
          'EX-9042'
         ),
         buildDatasetEntity(
          'ds-marketplace-q3',
          'Global Cloud Marketplace Q3',
          'cloud_marketplace_q3.json',
          'JSON Upload',
          SECONDARY_MARKETPLACE_RAW_ROWS,
          'EX-9038'
         )
        ]);
        setActiveDatasetId('ds-ecommerce-2026');
        showToast('Platform demo state restored to defaults', 'info');
       }}
      />
     )}
    </main>
   </div>
  </div>
 );
}