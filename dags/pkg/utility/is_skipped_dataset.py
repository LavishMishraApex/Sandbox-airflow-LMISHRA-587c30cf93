def is_skipped_dataset(d):
    if d == "feeder_temp" or d == 'looker_pdt' or d == 'salesforce' or(len(d) >= 8 and d[:8] == 'dataset_'):
        return True
    return False
