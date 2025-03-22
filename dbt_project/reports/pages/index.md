---
title: Welcome to Evidence
---

<Details title='How to edit this page'>

  This page can be found in your project at `/pages/index.md`. Make a change to the markdown file and save it to see the change take effect in your browser.
</Details>



## Nerd Facts Characters

```sql list_characters
SELECT 
  universe,
  COUNT(*) as character_count
FROM
  nerd_facts.characters_list
group by universe;
```

<DataTable data={list_characters}/>
