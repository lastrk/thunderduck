# ThunderDuck DataFrame API Roadmap

## Vision
Make ThunderDuck a production-ready drop-in replacement for Spark by achieving 100% DataFrame API compatibility for common operations.

## Current State (October 2025)
- **Coverage**: ~23% of TPC-H queries passing (5/22)
- **Core Operations**: Basic SELECT, JOIN, FILTER, GROUP BY work
- **Critical Gaps**: Function names, date/time, conditional aggregations

## Roadmap Phases

### Phase 1: Critical Fixes (Week 16 - Current)
**Goal**: Achieve 80% TPC-H query compatibility
**Timeline**: 5 days

#### Day 1 ✅ COMPLETED
- [x] Implement DEDUPLICATE support
- [x] Fix semi-join and anti-join patterns
- [x] Test infrastructure setup

#### Day 2 ✅ COMPLETED
- [x] Implement all 22 TPC-H queries in DataFrame API
- [x] Identify and document all gaps
- [x] Create comprehensive test suite

#### Day 3 (Tomorrow)
- [ ] Implement function name mapping layer
  - `endswith` → `ends_with`
  - `startswith` → `starts_with`
  - Create extensible mapping system
- [ ] Fix date/time extraction functions
  - `year()`, `month()`, `day()`
  - Date arithmetic support

#### Day 4
- [ ] Fix conditional aggregations
  - `when().otherwise()` in aggregations
  - Complex CASE expressions
- [ ] String function enhancements
  - `contains()`, `substring()`
  - Regular expression support

#### Day 5
- [ ] Comprehensive testing of all 22 queries
- [ ] Documentation and workarounds guide
- [ ] Performance baseline establishment

**Expected Outcome**: 18/22 queries passing (82%)

### Phase 2: Enhanced Compatibility (Week 17)
**Goal**: Achieve 95% DataFrame API coverage
**Timeline**: 5 days

#### Features to Implement
1. **Window Functions** (2 days)
   - Row number, rank, dense rank
   - Partitioning and ordering
   - Frame specifications
   - Lead/lag functions

2. **Advanced Aggregations** (1 day)
   - ROLLUP and CUBE
   - GROUPING SETS
   - Multiple aggregation strategies

3. **Complex Subqueries** (1 day)
   - Correlated subqueries
   - Scalar subqueries
   - EXISTS/NOT EXISTS patterns

4. **Data Type Enhancements** (1 day)
   - Array functions
   - Map functions
   - Struct operations

**Expected Outcome**: 21/22 queries passing (95%)

### Phase 3: Production Readiness (Week 18)
**Goal**: Production deployment ready
**Timeline**: 5 days

#### Focus Areas
1. **Performance Optimization** (2 days)
   - Query plan optimization
   - Pushdown predicates
   - Join reordering
   - Statistics collection

2. **Error Handling** (1 day)
   - Comprehensive error messages
   - Graceful fallbacks
   - Debug mode enhancements

3. **Monitoring & Observability** (1 day)
   - Query execution metrics
   - Performance profiling
   - Resource usage tracking

4. **Documentation** (1 day)
   - Complete API reference
   - Migration guide from Spark
   - Performance tuning guide

**Expected Outcome**: 22/22 queries passing (100%)

### Phase 4: Advanced Features (Weeks 19-20)
**Goal**: Exceed Spark compatibility with DuckDB-specific optimizations

#### Innovations
1. **DuckDB Native Features**
   - Leverage DuckDB's columnar execution
   - Advanced statistics usage
   - Native file format optimizations

2. **Spark 3.x Features**
   - Adaptive Query Execution (AQE)
   - Dynamic Partition Pruning
   - Join hints support

3. **Performance Enhancements**
   - Vectorized execution
   - Code generation optimizations
   - Memory management improvements

## Success Metrics

### Technical Metrics
- **Query Compatibility**: % of TPC-H queries passing
- **Function Coverage**: % of Spark functions supported
- **Performance**: Query execution time vs Spark
- **Memory Usage**: Peak memory vs Spark

### Business Metrics
- **Adoption Rate**: Number of teams using ThunderDuck
- **Migration Success**: % of Spark workloads migrated
- **Cost Savings**: Infrastructure cost reduction
- **Developer Satisfaction**: NPS score

## Risk Mitigation

### Known Risks
1. **Complex UDFs**: May require significant rework
2. **Spark ML Integration**: Out of scope initially
3. **Streaming Operations**: Not supported by design

### Mitigation Strategies
1. **Incremental Migration**: Support mixed Spark/ThunderDuck deployments
2. **Escape Hatches**: Allow SQL passthrough for unsupported operations
3. **Community Engagement**: Open source contributions for edge cases

## Dependencies

### Technical Dependencies
- DuckDB version compatibility
- Arrow format alignment
- gRPC protocol stability

### Resource Dependencies
- 2 engineers for core development
- 1 engineer for testing
- 0.5 engineer for documentation

## Milestones

| Date | Milestone | Success Criteria |
|------|-----------|-----------------|
| Week 16 End | Basic Compatibility | 80% queries pass |
| Week 17 End | Enhanced Features | 95% queries pass |
| Week 18 End | Production Ready | 100% queries pass |
| Week 20 End | Advanced Features | Outperform Spark |

## Long-term Vision (6 months)

### Q1 2026
- Full Spark 3.x DataFrame API compatibility
- Production deployments at scale
- Open source community established

### Q2 2026
- Performance leadership vs Spark
- Cloud-native deployment options
- Enterprise support offerings

## Call to Action

### Immediate Next Steps
1. Fix function name mappings (2 hours)
2. Implement date/time functions (4 hours)
3. Test all 22 queries systematically
4. Create user migration guide

### How to Contribute
1. Test with your Spark workloads
2. Report gaps and issues
3. Contribute function implementations
4. Share performance benchmarks

## Conclusion

ThunderDuck's DataFrame API is on track to become a viable Spark alternative. With focused execution on this roadmap, we can achieve production readiness within 3 weeks and performance leadership within 6 months.

---

*Last Updated: October 29, 2025*
*Version: 1.0*
*Status: Active Development*