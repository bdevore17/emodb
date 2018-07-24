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
    }

    @Override
    public boolean hasNext() {
        return _iterator.hasNext() && (_minimum > 0 || System.currentTimeMillis() < _expireAt);
    }

    @Override
    public T next() {
        if (hasNext()) {
            if (_minimum > 0) {
                _minimum--;
            }
            return _iterator.next();
        }
        throw new NoSuchElementException();
    }
}
