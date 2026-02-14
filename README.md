# 💳 Finance Unified Base

> **AI-Powered Financial Data Pipeline** | Transforming unstructured credit card statements into analytics-ready datasets

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-REST_API-lightgrey.svg)](https://flask.palletsprojects.com/)
[![OpenAI](https://img.shields.io/badge/OpenAI-API-412991.svg)](https://openai.com/)
[![GCP](https://img.shields.io/badge/Google_Cloud-Storage-4285F4.svg)](https://cloud.google.com/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg)](https://www.docker.com/)

---

## 🎯 Overview

An end-to-end **data engineering solution** that automates the extraction, transformation, and storage of financial data from credit card statements. Built to eliminate manual data entry and enable real-time financial analytics.

**Live Impact**: Reduces data processing time from **hours to minutes**, enabling immediate insights for financial decision-making.

---

## ✨ Key Features

- 🤖 **AI-Powered Extraction**: Leverages OpenAI API for intelligent PDF parsing and data extraction
- 📊 **Automated ETL Pipeline**: Complete workflow from email ingestion to analytics-ready tables
- ☁️ **Cloud-Native Architecture**: Scalable data lake on Google Cloud Storage with Bronze/Silver/Gold tiers
- 🔒 **Secure Processing**: Handles password-protected PDFs and sensitive financial data
- 🐳 **Production-Ready**: Containerized with Docker for consistent deployments
- 🔄 **Workflow Orchestration**: Integrated with n8n for automated scheduling and monitoring

---

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Gmail     │────▶│   n8n        │────▶│   Flask     │────▶│   GCS Data   │
│  Ingestion  │     │ Orchestrator │     │   REST API  │     │     Lake     │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
                                                │
                                                ▼
                                          ┌──────────────┐
                                          │   OpenAI     │
                                          │     API      │
                                          └──────────────┘
```

### Data Flow Pipeline

1. **📥 Extraction** → Email automation pulls statements from Gmail
2. **🔓 Unlock** → Python endpoint removes PDF passwords
3. **🧠 Parse** → OpenAI API intelligently extracts transaction data
4. **📋 Structure** → Transform semi-structured JSON to normalized tables
5. **💾 Store** → Persist data in GCS with medallion architecture (Bronze → Silver → Gold)
6. **📈 Analytics** → Denormalized tables ready for dashboards and reporting

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Backend** | Python 3.9+, Flask | REST API development |
| **AI/ML** | OpenAI GPT API | Intelligent document parsing |
| **Cloud Storage** | Google Cloud Storage | Scalable data lake |
| **Orchestration** | n8n | Workflow automation |
| **Containerization** | Docker, Docker Compose | Deployment & portability |
| **Data Format** | Parquet, JSON, CSV | Efficient storage & analytics |
| **Security** | API Key Auth, PDF encryption | Secure data handling |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Google Cloud Platform account
- OpenAI API key

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Crismar12/finance-base.git
   cd finance-base
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials:
   # - OPENAI_API_KEY
   # - GCP credentials
   # - API_KEY for endpoint protection
   ```

3. **Run with Docker**
   ```bash
   docker-compose up --build
   ```

4. **Or run locally**
   ```bash
   pip install -r requirements.txt
   python api_src/app.py
   ```

---

## 📡 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/remove-password` | POST | Unlock password-protected PDFs |
| `/parse-document` | POST | Extract structured data from PDFs |
| `/structure-data` | POST | Transform JSON to tabular format |
| `/process-data` | POST | Create analytics-ready datasets |

**Authentication**: Include `X-API-Key` header in all requests.

---

## 📊 Data Lake Structure

```
gs://bucket-name/
├── bronze/
│   ├── landing/              # Raw ingested files
│   └── raw/                  # Processed PDFs, JSON, CSV
├── silver/
│   └── discovery/            # Normalized tables
└── gold/
    └── analytics/            # Denormalized, dashboard-ready
```

**Partitioning Strategy**: `provider_type/provider_name/card_name=smart-visa/`

---

## 💼 Business Value

- ⏱️ **Time Savings**: Automated processing reduces manual work by ~90%
- 📊 **Real-Time Analytics**: Financial data available within minutes of statement receipt
- 🔄 **Scalability**: Cloud-native architecture handles growing data volumes
- 🎯 **Accuracy**: AI-powered extraction minimizes human error
- 💰 **Cost Efficiency**: Parquet format reduces storage costs by ~80%

---

## 🧪 Testing

```bash
# Run unit tests
python -m pytest tests/

# Run with coverage
python -m pytest --cov=api_src tests/
```

---

## 📝 Use Cases

- 💳 Personal finance tracking and budgeting
- 📈 Expense trend analysis and forecasting
- 🏢 Small business financial reporting
- 🔍 Transaction categorization and insights
- 📊 Integration with BI tools (Tableau, Power BI, Looker)

---

## 🔐 Security

- API key authentication for all endpoints
- Secure handling of financial documents
- GCP service account with minimal required permissions
- No hardcoded credentials (environment variables only)
- Pre-commit hooks for code quality and security checks

---

## 🗺️ Roadmap

- [ ] Support for additional banks and credit card providers
- [ ] Machine learning for transaction categorization
- [ ] Real-time dashboard integration
- [ ] Multi-currency support
- [ ] Automated anomaly detection
- [ ] Mobile app for on-the-go access

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. See our [Pull Request Template](PULL_REQUEST_TEMPLATE.md) for guidelines.

---

## 📄 License

This project is open source and available under the MIT License.

---

## 👤 Author

**Crismar12**

- GitHub: [@Crismar12](https://github.com/Crismar12)
- LinkedIn: [Connect with me](https://www.linkedin.com/in/your-profile) <!-- Add your LinkedIn -->

---

## 🌟 Show Your Support

If you find this project useful, please consider giving it a ⭐️ on GitHub!

---

## 📬 Contact

For questions, feedback, or collaboration opportunities, feel free to open an issue or reach out directly.

---

<div align="center">
  <strong>Built with ❤️ for smarter financial data management</strong>
</div>