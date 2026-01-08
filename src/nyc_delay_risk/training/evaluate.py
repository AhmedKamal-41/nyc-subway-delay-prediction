from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
)
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import numpy as np


def compute_metrics(y_true, y_pred, y_pred_proba):
    """Compute classification metrics."""
    metrics = {
        'accuracy': accuracy_score(y_true, y_pred),
        'precision': precision_score(y_true, y_pred, zero_division=0),
        'recall': recall_score(y_true, y_pred, zero_division=0),
        'f1': f1_score(y_true, y_pred, zero_division=0),
        'roc_auc': roc_auc_score(y_true, y_pred_proba) if len(np.unique(y_true)) > 1 else 0.0
    }
    return metrics


def plot_confusion_matrix(y_true, y_pred, model_name, save_path):
    """Create and save confusion matrix plot."""
    cm = confusion_matrix(y_true, y_pred)
    
    fig, ax = plt.subplots(figsize=(8, 6))
    im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
    ax.figure.colorbar(im, ax=ax)
    
    ax.set(xticks=np.arange(cm.shape[1]),
           yticks=np.arange(cm.shape[0]),
           xticklabels=['No Alert', 'Alert'],
           yticklabels=['No Alert', 'Alert'],
           title=f'Confusion Matrix - {model_name}',
           ylabel='True Label',
           xlabel='Predicted Label')
    
    thresh = cm.max() / 2.0
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(j, i, format(cm[i, j], 'd'),
                   ha="center", va="center",
                   color="white" if cm[i, j] > thresh else "black")
    
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()

