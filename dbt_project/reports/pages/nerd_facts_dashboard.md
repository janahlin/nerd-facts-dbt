---
title: Nerd Facts Dashboard
---

## Universe Statistics

```sql facts_summary
SELECT universe, 
character_count,
location_count,
total_count
from nerd_facts.facts_summary;
```

<DataTable data={facts_summary}/>