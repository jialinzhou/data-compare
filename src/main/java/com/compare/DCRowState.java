package com.compare;

public enum DCRowState {

    INSERTED,
    DELETED,
    MODIFIED;

    private DCRowState() {
    }

    public boolean isRelated(boolean left) {
        return this != (left ? INSERTED : DELETED);
    }

    public DCRowState getRelativeTo(boolean left) {
        switch (this) {
            case INSERTED:
                return left ? INSERTED : DELETED;
            case DELETED:
                return left ? DELETED : INSERTED;
            default:
                return this;
        }
    }
}
