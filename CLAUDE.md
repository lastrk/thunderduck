# Claude Code Project Rules

This file contains project-specific rules and guidelines for working with catalyst2sql.

## Documentation Structure Rules

**Permanent Rule**: The catalyst2sql project follows a strict documentation structure pattern:

1. **ONE high-level plan** (`IMPLEMENTATION_PLAN.md`)
   - Contains the 16-week development roadmap
   - Describes all phases and major milestones
   - This is the single source of truth for project timeline

2. **ONE detailed plan per active milestone** (e.g., `WEEK11_IMPLEMENTATION_PLAN.md`)
   - Contains the detailed 5-day implementation plan for the current week
   - Includes specific tasks, test coverage, and success criteria
   - Only one detailed plan should exist at a time for the current milestone
   - Previous week detailed plans should be removed or consolidated

3. **ONE completion report per finished milestone** (e.g., `WEEK*_COMPLETION_REPORT.md`)
   - Historical record of completed work
   - Documents achievements, test results, and lessons learned
   - Should be preserved for project history

4. **Everything else consolidated or removed**
   - No duplicate high-level plans
   - No obsolete planning documents
   - Architecture documentation goes in `docs/architect/`
   - Protocol specifications go in `docs/`
   - Testing documentation goes in `docs/Testing_Strategy.md`

### Enforcement

When creating new documentation:
- **DO NOT** create additional high-level implementation plans
- **DO NOT** create duplicate milestone plans
- **DO** consolidate temporary planning documents into the appropriate permanent location
- **DO** remove obsolete planning documents after content is extracted
- **DO** create completion reports when milestones are finished
- **DO** archive or remove the detailed plan after the completion report is created

### Current Structure

```
catalyst2sql/
├── IMPLEMENTATION_PLAN.md              # ONE high-level plan
├── README.md                            # Project overview
├── WEEK11_IMPLEMENTATION_PLAN.md        # Current milestone (active)
├── WEEK*_COMPLETION_REPORT.md           # Historical completion reports
├── docs/
│   ├── SPARK_CONNECT_PROTOCOL_SPEC.md  # Protocol reference
│   ├── Testing_Strategy.md              # Testing approach
│   ├── architect/                       # Architecture documentation
│   └── coder/                           # Build/CI documentation
```

**Last Updated**: 2025-10-17
**Cleanup Report**: See `DOCUMENTATION_CLEANUP_PLAN.md` for the rationale behind this structure
