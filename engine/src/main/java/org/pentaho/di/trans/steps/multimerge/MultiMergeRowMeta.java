package org.pentaho.di.trans.steps.multimerge;

import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

public class MultiMergeRowMeta extends RowMeta {

    private final boolean duplicateFieldsAllowed;

    public MultiMergeRowMeta(final boolean duplicateFieldsAllowed) {
        super();
        this.duplicateFieldsAllowed = duplicateFieldsAllowed;
    }

    @Override
    public void mergeRowMeta(RowMetaInterface r, String originStepName) {
        lock.writeLock().lock();
        try {
            if (duplicateFieldsAllowed) {
                for (int x = 0; x < r.size(); x++) {
                    final ValueMetaInterface field = r.getValueMeta(x);
                    if (searchValueMeta(field.getName()) == null) {
                        addValueMeta(field); // Not in list yet: add
                    } else {
                        // We want to rename the field to Name[2], Name[3], ...
                        //
                        addValueMeta(renameValueMetaIfInRow(field, originStepName));
                    }
                }
            } else {
                for (int x = 0; x < r.size(); x++) {
                    final ValueMetaInterface field = r.getValueMeta(x);
                    final String name = field.getName();
                    if (indexOfValue(name) == -1) {
                        addValueMeta(field);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}
