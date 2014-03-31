package org.belliottsmith;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PauselessHashMap<K, V> implements Map<K, V>
{

    private static final class Entry<K, V>
    {
        final K key;
        final int hash;
        V value;
        Entry<K, V> next;
        private Entry(K key, V value, int hash)
        {
            this.key = key;
            this.value = value;
            this.hash = hash;
        }
    }

    private static ExecutorService allocator = Executors.newFixedThreadPool(1);

    private int migrated;
    private Entry<K, V>[] oldTable;
    private Entry<K, V>[] table;
    private Future<Entry<K, V>[]> nextTable;

    private int size;
    private int nextResize;

    public PauselessHashMap(int initialSize)
    {
        initialSize = Math.max(16, initialSize);
        initialSize = 1 << (31 - Integer.numberOfLeadingZeros(initialSize));
        table = new Entry[initialSize];
        nextResize = (int) (0.5f * initialSize);
    }

    private static int hash(Object key)
    {
        return key.hashCode();
    }

    private void migrate(int index, int newMask)
    {
        Entry<K, V> entry = oldTable[index];
        while (entry != null)
        {
            Entry<K, V> next = entry.next;
            int newIndex = entry.hash & newMask;
            entry.next = table[newIndex];
            table[newIndex] = entry;
            entry = next;
        }
        oldTable[index] = entry;
    }

    public V put(K key, V value)
    {
        assert key != null;
        int hash = hash(key);
        int mask = table.length - 1;
        if (oldTable != null)
        {
            for (int i = 0 ; i < 4 ; i++)
                migrate(migrated++, mask);
            if (migrated == oldTable.length)
                oldTable = null;
            else if ((hash & (mask >> 1)) < migrated)
                migrate(hash & (mask >> 1), mask);
        }
        int index = hash & mask;
        Entry<K, V> entry = table[index], prec = null;
        while (entry != null)
        {
            if (entry.hash == hash && entry.key.equals(key))
            {
                V prev = entry.value;
                entry.value = value;
                return prev;
            }
            prec = entry;
            entry = entry.next;
        }
        if (prec == null)
            table[index] = new Entry<K, V>(key, value, hash);
        else
            prec.next = new Entry<K, V>(key, value, hash);
        if (++size > nextResize && oldTable == null)
        {
            if (table.length < 1024)
            {
                nextResize <<= 1;
                oldTable = table;
                migrated = 0;
                table = new Entry[table.length << 1];
            }
            else if (nextTable == null)
            {
                nextTable = allocator.submit(new Callable<Entry<K, V>[]>()
                {
                    public Entry<K, V>[] call() throws Exception
                    {
                        return new Entry[table.length << 1];
                    }
                });
            }
            else if (nextTable.isDone())
            {
                try
                {
                    nextResize <<= 1;
                    oldTable = table;
                    migrated = 0;
                    table = nextTable.get();
                }
                catch (InterruptedException e)
                {
                    throw new IllegalStateException();
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                nextTable = null;
            }
        }
        return null;
    }

    public V remove(Object key)
    {
        return null;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {

    }

    public void clear()
    {

    }

    public Set<K> keySet()
    {
        return null;
    }

    public Collection<V> values()
    {
        return null;
    }

    public Set<Map.Entry<K, V>> entrySet()
    {
        return null;
    }

    public int size()
    {
        return 0;
    }

    public boolean isEmpty()
    {
        return false;
    }

    public boolean containsKey(Object key)
    {
        return false;
    }

    public boolean containsValue(Object value)
    {
        return false;
    }

    public V get(Object key)
    {
        int hash = hash(key);
        int mask = table.length - 1;
        Entry<K, V> chain;
        if (oldTable != null && (hash & (mask >> 1)) < migrated)
            chain = oldTable[hash & (mask >> 1)];
        else
            chain = table[hash & mask];
        while (chain != null)
            if (chain.hash == hash && chain.key.equals(key))
                return chain.value;
        return null;
    }

    public static void main(String[] args)
    {
        for (int i = 0 ; i < 10 ; i++)
            test2(1000000);
        for (int i = 0 ; i < 10 ; i++)
            test2(10000000);
        for (int i = 0 ; i < 10 ; i++)
            test(1000000);
        for (int i = 0 ; i < 10 ; i++)
            test(10000000);
    }

    final static long[] latencies = new long[1 << 25];
    private static void test2(int count)
    {
        final PauselessHashMap<Integer, Integer> map = new PauselessHashMap<Integer, Integer>(16);
        System.gc();
        for (int i = 0 ; i < count ; i++)
        {
            long start = System.nanoTime();
            map.put(i, i);
            latencies[i] = System.nanoTime() - start;
        }
        long[] print = Arrays.copyOfRange(latencies, 0, count);
        Arrays.sort(print);
        System.out.println("Mine:");
        System.out.println(String.format("%d %d %d %d %d", perc(0.5f, print), perc(0.95f, print), perc(0.99f, print), perc(0.999f, print), perc(0.99999f, print)));
        System.out.println(String.format("-- %d %d %d %d %d", print[print.length - 1], print[print.length - 2], print[print.length - 3], print[print.length - 4], print[print.length - 5]));
    }

    private static void test(int count)
    {
        final org.giltene.PauselessHashMap map = new org.giltene.PauselessHashMap(16);
        System.gc();
        for (int i = 0 ; i < count ; i++)
        {
            long start = System.nanoTime();
            map.put(i, i);
            latencies[i] = System.nanoTime() - start;
        }
        long[] print = Arrays.copyOfRange(latencies, 0, count);
        Arrays.sort(print);
        System.out.println("Gil's:");
        System.out.println(String.format("%d %d %d %d %d", perc(0.5f, print), perc(0.95f, print), perc(0.99f, print), perc(0.999f, print), perc(0.99999f, print)));
        System.out.println(String.format("-- %d %d %d %d %d", print[print.length - 1], print[print.length - 2], print[print.length - 3], print[print.length - 4], print[print.length - 5]));
    }

    private static long perc(float perc, long[] vals)
    {
        return vals[Math.min(vals.length - 1, ((int) (perc * vals.length)))];
    }

}
