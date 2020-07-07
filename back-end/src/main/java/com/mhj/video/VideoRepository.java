package com.mhj.video;

import org.springframework.data.repository.CrudRepository;

import org.springframework.data.repository.CrudRepository;

/**
 * A simple Spring Data {@link CrudRepository} for storing {@link Video}s.
 *
 * @author Greg Turnquist
 */
interface VideoRepository extends CrudRepository<Video, Long> {
}