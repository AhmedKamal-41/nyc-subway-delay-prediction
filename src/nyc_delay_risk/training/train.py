from sklearn.linear_model import LogisticRegression
import lightgbm as lgb


def get_feature_columns(df):
    """Extract feature column names, excluding identifiers and label."""
    exclude_cols = {'fact_id', 'bucket_start', 'bucket_size_seconds', 
                    'line_id', 'stop_id', 'created_at', 'label'}
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    return feature_cols


def train_logistic_regression(X_train, y_train):
    """Train LogisticRegression with specified parameters."""
    model = LogisticRegression(
        solver='liblinear',
        class_weight='balanced',
        max_iter=200,
        random_state=42
    )
    model.fit(X_train, y_train)
    return model


def train_lightgbm(X_train, y_train, X_val, y_val):
    """Train LightGBM classifier with early stopping on validation set."""
    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
    
    params = {
        'objective': 'binary',
        'metric': 'binary_logloss',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'random_state': 42,
        'verbosity': -1
    }
    
    model = lgb.train(
        params,
        train_data,
        num_boost_round=500,
        valid_sets=[val_data],
        callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(0)]
    )
    
    return model

