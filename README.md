# DE03 Capstone Project: Taxi Pipeline  

This project is a data engineering pipeline that processes taxi trip data.

## Project Structure  

The repository contains the following directories and files:  

- **`data/`** – Contains raw taxi trip data files. (CSV & JSON)  
- **`result/`** – Stores output results after processing the data.  
- **`src/`** – Includes source code for data processing and analysis.  
- **`staging/`** – Used for intermediate data storage during processing.    

## Installation & Setup  

### 1. Clone the Repository  
```bash
git clone https://github.com/dimasadimo/DE03-Capstone-Project-taxi-pipeline.git
cd DE03-Capstone-Project-taxi-pipeline
```

### 2. Install Python and Pandas  
Ensure you have Python installed on your system. If not, download and install it from [python.org](https://www.python.org/downloads/).  

After installing Python, install Pandas using the following command:  
```bash
pip install pandas
```

## How to Run  

### 1. Run the Python Scripts  
Navigate to the `src/` directory and run the main script:  
```bash
cd src
python main.py
```