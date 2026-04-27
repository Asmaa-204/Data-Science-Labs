# README – Exercise 2: GXDataValidator (Great Expectations v1.x)


## 1. Installation

```bash
pip install great-expectations>=1.13 pandas numpy scipy
```

Verify GX version:

```python
import great_expectations as gx
print(gx.__version__)  # should be >= 1.13
```

---

## 2. How to Run

```bash
cd exercise2
python gx_data_validator.py
```

To regenerate the HTML Data Docs:

```bash
python gx_data_validator.py
# HTML files are saved to: exercise2/html_reports/
```

---

## 3. GX v1.x Workflow Summary

> *[Students: Briefly explain each step of the GX v1.x pipeline in your own words.]*

1. **Context** – `gx.get_context(mode="ephemeral")` creates an in-memory workspace. No files are written to disk.
2. **Data Source** – ...
3. **Data Asset** – ...
4. **Batch Definition** – ...
5. **Expectation Suite** – ...
6. **Validation Definition** – ...
7. **Run** – ...
8. **Data Docs** – ...

---

## 4. Expectation Mapping

> *[Students: Fill in the GX expectation class used for each validation dimension.]*

| Dimension | GX Expectation Used | Notes |
|-----------|---------------------|-------|
| Schema – column presence | `ExpectTableColumnsToMatchSet` | |
| Schema – data types | `ExpectColumnValuesToBeOfType` | |
| Completeness | `ExpectColumnValuesToNotBeNull` | |
| Ranges (Accuracy) | `ExpectColumnValuesToBeBetween` | |
| Uniqueness | `ExpectColumnValuesToBeUnique` | |
| Categorical | `ExpectColumnValuesToBeInSet` | |
| Outliers | ... | Approximated using ... |
| Timeliness | ... | |
| Distribution | ... | |
| Correlations | ... | GX limitation: see section 5 |

---

## 5. Limitations of GX for Some Dimensions

> *[Students: Explain which dimensions GX cannot fully cover natively and how you worked around them.]*

- **Isolation Forest**: GX has no native multivariate anomaly-detection expectation. I approximated it by ...
- **KS Test / Distribution Drift**: ...
- **Pearson / Spearman Correlation**: ...

---

## 6. Dataset Findings

### Dataset 1 – `ecommerce_orders.csv`

1. ...
2. ...
3. ...
4. ...
5. ...

### Dataset 2 – `patient_health_records.csv`

1. ...
2. ...
3. ...
4. ...
5. ...

---

## 7. Screenshots – HTML Data Docs

**Screenshot 1 – ecommerce Data Docs:**

![ecommerce GX Docs](screenshots/ex2_orders_docs.png)

**Screenshot 2 – health Data Docs:**

![health GX Docs](screenshots/ex2_health_docs.png)
