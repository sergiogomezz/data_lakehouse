# 🚀 Final Bachelor's Project – BSc in Computer Engineering

**Title:** Scalable and Transactional Data Pipeline for Real-Time Processing  
**Institution:** University of the Basque Country (UPV/EHU)  
**In collaboration with:** [IKERLAN Technology Center](https://www.ikerlan.es/en)  
**Awarded by:** iGazte Program – UPV/EHU  

---

## 🧠 Project Overview

This project tackles one of the most pressing challenges in modern data engineering: building a scalable and transactional data pipeline capable of handling large volumes of real-time data efficiently and consistently.

In collaboration with the industrial R&D center IKERLAN, the pipeline is designed and deployed in a **Cloud-based architecture**, culminating in a **Data Lakehouse**—an architecture that merges the flexibility of Data Lakes with the performance of Data Warehouses.

---

## 🏗️ Architecture & Technologies

The pipeline is structured across several modular stages:

1. **Data Ingestion**
   - Tools: Apache Kafka, MQTT, REST APIs  
   - Goal: Handle high-throughput event-driven data streams

2. **Data Processing**
   - Technologies: Apache Spark, dbt
   - Focus: Real-time transformation and cleaning with scalability in mind

3. **Data Storage**
   - Solution: Apache Hudi (Lakehouse)  
   - Features: ACID transactions, schema enforcement, time travel

4. **Deployment**
   - Infrastructure: AWS CDK
   - CI/CD pipeline for iterative testing and deployment

---

## 🎯 Objectives

- ✅ Build a modular and cloud-native data pipeline  
- ✅ Ensure **transactional guarantees** (ACID) during ingestion and transformation  
- ✅ Enable **real-time** data processing  
- ✅ Achieve scalability across components  
- ✅ Evaluate and benchmark technology choices throughout development

---

## 📊 Impact & Applications

This pipeline serves as a blueprint for scalable data systems in industrial environments, supporting use cases such as:

- Predictive maintenance
- Industrial IoT analytics
- Real-time dashboards and monitoring systems

---

## 🔍 Research Contributions

- Evaluation of modern storage formats (Hudi, Delta, Iceberg)  
- Benchmarking ingestion tools under high-concurrency scenarios  
- Trade-offs between lakehouse and traditional data warehouse models

---

## 📌 Future Work

- Advanced monitoring with Prometheus + Grafana  
- Auto-scaling strategies and fault-tolerance improvements

---

## 🙌 Acknowledgements

This work was made possible thanks to the support of:

- **IKERLAN Technology Center**, for their industrial insight and infrastructure
- **iGazte Program (UPV/EHU)**, for funding the project and promoting student-led innovation
