package com.bazaarvoice.emodb.sor.client;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

class PeekingIterator<E> implements Iterator {

    private final Iterator<? extends E> iterator;
    private boolean hasPeeked;
    private E peekedElement;

    public PeekingIterator(Iterator<? extends E> iterator) {
        this.iterator = requireNonNull(iterator);
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || iterator.hasNext();
    }

    @Override
    public E next() {
        if (!hasPeeked) {
            return iterator.next();
        }
        E result = peekedElement;
        hasPeeked = false;
        peekedElement = null;
        return result;
    }

    @Override
    public void remove() {
        if (hasPeeked) {
            throw new IllegalStateException("Can't remove after you've peeked at next");
        }
        iterator.remove();
    }

    public E peek() {
        if (!hasPeeked) {
            peekedElement = iterator.next();
            hasPeeked = true;
        }
        return peekedElement;
    }
}
