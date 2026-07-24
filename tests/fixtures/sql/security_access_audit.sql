-- Security access audit for privileged roles.
DECLARE @AsOfDate datetime2(0) = SYSUTCDATETIME();
DECLARE @IncludeDisabled bit = 0;

WITH privileged_roles AS (
    SELECT
        r.RoleId,
        r.RoleName
    FROM security.Roles AS r
    WHERE
        r.RoleName IN ('sysadmin', 'securityadmin', 'data_owner')
        OR r.IsPrivileged = 1
),
latest_login AS (
    SELECT
        al.UserId,
        MAX(al.LoginAt) AS LastLoginAt
    FROM security.AccessLog AS al
    WHERE
        al.LoginAt <= @AsOfDate
    GROUP BY
        al.UserId
)
SELECT
    u.UserId,
    u.LoginName,
    pr.RoleName,
    ll.LastLoginAt,
    CASE
        WHEN ll.LastLoginAt IS NULL THEN 'Never logged in'
        WHEN ll.LastLoginAt < DATEADD(day, -90, @AsOfDate) THEN 'Stale'
        ELSE 'Recent'
    END AS LoginStatus
FROM security.Users AS u
INNER JOIN security.UserRoles AS ur
    ON ur.UserId = u.UserId
INNER JOIN privileged_roles AS pr
    ON pr.RoleId = ur.RoleId
LEFT JOIN latest_login AS ll
    ON ll.UserId = u.UserId
WHERE
    (@IncludeDisabled = 1 OR u.DisabledAt IS NULL)
    AND NOT EXISTS (
        SELECT
            1
        FROM security.AccessExceptions AS ae
        WHERE
            ae.UserId = u.UserId
            AND ae.ExpiresAt > @AsOfDate
    )
ORDER BY
    pr.RoleName,
    u.LoginName;

EXEC audit.WriteAccessAuditHeartbeat @CheckedAt = @AsOfDate;
