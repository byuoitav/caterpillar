### Basic workflow

Basic workflow: I'm given a query that will get the data I need - but does NOT have a timeperiod associated with it. The config for the job denotes an interval for running, and I'll have to store some information from run to run (so we know exactly how far we got last time. Don't want to lose intermediate data.) In addition we need to pass in the 'timestamp field' that we use. Then The service will do an initial query to see how many items there are, if it's not too many (say <1000) it'll just pull them all down and pass them to the system. Otherwise there will be a function passed in to each 'job' that is the 'get more' function. Like a read closer it'll get the next X minutes (some breakdownable unit) until all the records are gotten.

So the run interface needs to look like this

```
Run(id string, recordCount int, start, end time.Time, GetData func()[]interface{}, DocsChannel chan UpdateRecord, config JobConfig, state interface{}) (*nerr.E, interface{})
```

Docs channel will send in an update record (similar to the type defined in the state-parser) that will be aggregated and passed to a bulk update deal.
