package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.transaction.annotation.Transactional;

public interface UserRepository extends CrudRepository<UserRecord, Long> {
    UserRecord findById(long id);

    @Transactional
    @Modifying
    @Query("UPDATE UserRecord u SET u.balance = ?2 WHERE u.id = ?1")
    void updateBalance(long userId, float newBalance);
}
