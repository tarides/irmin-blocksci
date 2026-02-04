// queries.cypher - Cypher equivalents for BlockSci C++ tests
//
// These queries correspond to Table 7 in the BlockSci paper:
// "BlockSci: Design and applications of a blockchain analysis platform"
// (USENIX Security 2020)
//
// Usage:
//   cypher-shell -u neo4j -p blocksci -d neo4j < queries.cypher
//

// =============================================================================
// Paper Queries (Table 7)
// =============================================================================

// --- Tx locktime > 0 ---
// Count transactions with non-zero locktime
MATCH (t:Transaction)
WHERE t.locktime > 0
RETURN count(t) AS value;

// --- Max output value ---
// Find maximum output value across all outputs
MATCH (o:Output)
RETURN max(o.value) AS value;

// --- Calculate fee ---
// Find maximum transaction fee
// Note: The paper computes fee as sum(inputs) - sum(outputs), but we store
// pre-computed fee on Transaction nodes for efficiency
MATCH (t:Transaction)
RETURN max(t.fee) AS value;

// --- Satoshi Dice address ---
// Sum of outputs sent to a specific address (Satoshi Dice)
// Note: Replace ADDRESS_ID with the actual internal address ID
// MATCH (a:Address)<-[:TO_ADDRESS]-(o:Output)
// WHERE id(a) = ADDRESS_ID
// RETURN sum(o.value) AS value;

// --- Zero-conf outputs ---
// Count outputs spent within the same block
MATCH (t1:Transaction)-[:TX_OUTPUT]->(o:Output)<-[:TX_INPUT]-(t2:Transaction)
WHERE t1.blockHeight = t2.blockHeight
RETURN count(o) AS value;

// --- Locktime change ---
// Count transactions where exactly one output is spent by a transaction
// with matching locktime behavior (both zero or both non-zero)
MATCH (t1:Transaction)-[:TX_OUTPUT]->(o:Output)<-[:TX_INPUT]-(t2:Transaction)
WHERE (t1.locktime > 0) = (t2.locktime > 0)
WITH t1, count(o) AS matching_count
WHERE matching_count = 1
RETURN count(t1) AS value;


// =============================================================================
// Additional Queries (not in Table 7)
// =============================================================================

// --- Sum output value ---
// Sum of all output values
MATCH (o:Output)
RETURN sum(o.value) AS value;

// --- Sum fees ---
// Sum of all transaction fees
MATCH (t:Transaction)
RETURN sum(t.fee) AS value;

// --- Max input value ---
// Find maximum value of any spent output
MATCH ()-[:TX_INPUT]->(o:Output)
RETURN max(o.value) AS value;

// --- Tx version > 1 ---
// Count transactions with version > 1
MATCH (t:Transaction)
WHERE t.version > 1
RETURN count(t) AS value;

// --- Input count ---
// Count total number of inputs
MATCH ()-[r:TX_INPUT]->()
RETURN count(r) AS value;

// --- Output count ---
// Count total number of outputs
MATCH (o:Output)
RETURN count(o) AS value;


// =============================================================================
// Basic Counts
// =============================================================================

// --- Block count ---
MATCH (b:Block)
RETURN count(b) AS value;

// --- Transaction count ---
MATCH (t:Transaction)
RETURN count(t) AS value;

// --- Address count ---
MATCH (a:Address)
RETURN count(a) AS value;

// --- Spent outputs ---
MATCH (o:Output)<-[:TX_INPUT]-()
RETURN count(o) AS value;

// --- Unspent outputs (UTXOs) ---
MATCH (o:Output)
WHERE NOT (o)<-[:TX_INPUT]-()
RETURN count(o) AS value;


// =============================================================================
// Block Statistics
// =============================================================================

// --- Avg tx per block ---
MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
WITH b, count(t) AS txCount
RETURN avg(txCount) AS value;

// --- Max tx per block ---
MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
WITH b, count(t) AS txCount
RETURN max(txCount) AS value;


// =============================================================================
// Advanced Queries
// =============================================================================

// --- High-value tx ---
// Transactions with fee > 10 BTC (1,000,000,000 satoshis)
MATCH (t:Transaction)
WHERE t.fee > 1000000000
RETURN count(t) AS value;

// --- Multi-input tx ---
// Transactions with more than 10 inputs
MATCH (t:Transaction)-[:TX_INPUT]->()
WITH t, count(*) AS inputs
WHERE inputs > 10
RETURN count(t) AS value;


// =============================================================================
// Verification Queries
// =============================================================================

// --- Genesis block ---
MATCH (b:Block {height: 0})
RETURN b.hash AS genesis_hash, b.timestamp AS genesis_timestamp;

// --- Block 170 (first real transaction) ---
MATCH (b:Block {height: 170})-[:CONTAINS]->(t:Transaction)
RETURN count(t) AS tx_count;

// --- Script type distribution ---
MATCH (o:Output)
RETURN o.scriptType AS script_type, count(o) AS count
ORDER BY count DESC;

// --- Address type distribution ---
MATCH (a:Address)
RETURN a.type AS address_type, count(a) AS count
ORDER BY count DESC;
