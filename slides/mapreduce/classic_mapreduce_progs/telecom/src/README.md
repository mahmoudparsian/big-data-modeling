# src

Java source (Driver/Mapper/Reducer) for the classic `telecom` MapReduce program.

## Contents

| Name | Type | Description |
|---|---|---|
| [`TelecomDriver.java`](TelecomDriver.java) | java (1.5KB) | Job driver: configures and submits the CDR-analytics MapReduce job |
| [`TelecomMapper.java`](TelecomMapper.java) | java (2.2KB) | Parses each CDR record and computes STD-call duration |
| [`TelecomReducer.java`](TelecomReducer.java) | java (887B) | Sums STD-call minutes per phone number |
