package com.bazaarvoice.emodb.sor.client;

import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Wraps an iterator such that {@link #hasNext()} returns {@code false} after a certain period of time.
 * <p>
 * A minimum may be specified such that the time limit is ignored until at least {@code N} items have been returned.
 */
class TimeLimitedIterator<T> implements Iterator<T> {
    private final Iterator<T> _iterator;
    private final long _expireAt;
    private long _minimum;

    private boolean _computedNextElement;
    private T _nextElement;

    static <T> TimeLimitedIterator<T> create(Iterator<T> iterator, Duration duration, long minimum) {
        return new TimeLimitedIterator<>(iterator, duration, minimum);
    }

    private TimeLimitedIterator(Iterator<T> iterator, Duration duration, long minimum) {
        _iterator = requireNonNull(iterator, "iterator");
        _expireAt = System.currentTimeMillis() + duration.toMillis();
        if (minimum < 0) {
            throw new IllegalArgumentException("Minimum must be >= 0");
        }
        _minimum = minimum;
        _computedNextElement = false;
    }

    @Override
    public boolean hasNext() {
        if (_computedNextElement) {
            return true;
        }

        return computeNextElement();
    }

    @Override
    public T next() {

        if (_computedNextElement || computeNextElement()) {
            _computedNextElement = false;
            return _nextElement;
        }

        throw new NoSuchElementException();

    }

    private boolean computeNextElement() {
        if (_iterator.hasNext() && (_minimum > 0 || System.currentTimeMillis() < _expireAt)) {
            _computedNextElement = true;
            _nextElement = _iterator.next();
            if (_minimum > 0) {
                _minimum--;
            }
            return true;
        }
         return false;
    }
}
