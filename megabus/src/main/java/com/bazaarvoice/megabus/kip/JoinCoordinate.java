package com.bazaarvoice.megabus.kip;

class JoinCoordinate<K, K0> {
    private K key;
    private K0 foreignKey;

    JoinCoordinate(K key, K0 foreignKey) {
        this.key = key;
        this.foreignKey = foreignKey;
    }

    public K getKey() {
        return key;
    }

    public K0 getForeignKey() {
        return foreignKey;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setForeignKey(K0 foreignKey) {
        this.foreignKey = foreignKey;
    }
}
